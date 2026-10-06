from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import delete
from sqlalchemy.orm import Session

from position_guardian.alert_contract import GUARDIAN_SHADOW_CONTEXT_ALERT_SOURCE_V1
from position_guardian.alert_recovery import recover_guardian_alerts
from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.models import GuardianAlertOutboxRow, PrivateSnapshotCursorRow
from position_guardian.persistence.repository import (
    GuardianAlertOutboxConflictError,
    GuardianEventConflictError,
    GuardianRepository,
)
from position_guardian.reconcile import ReconciliationRequest, reconcile_once


def _identity() -> ManagedPositionIdentity:
    return ManagedPositionIdentity("private-account-sentinel", "BTCUSDT", "LONG", 3)


def _candidate() -> AdoptionCandidate:
    return AdoptionCandidate(
        identity=_identity(),
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        source_update_time_ms=2000,
        original_risk_stop=Decimal("59000"),
        protection_floor=Decimal("60500"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference("open_order", 11, Decimal("59000")),
    )


def _position(*, amount: str = "0.01", update_time_ms: int = 3000) -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amount=Decimal(amount),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        unrealized_profit=Decimal("10"),
        update_time_ms=update_time_ms,
    )


def _request(
    *,
    event_id: str,
    amount: str = "0.01",
    protective: bool = True,
    uncertainty: str = "CERTAIN",
    release_event_id: str | None = None,
    event_time_ms: int = 3000,
) -> ReconciliationRequest:
    return ReconciliationRequest(
        identity=_identity(),
        position=_position(amount=amount),
        snapshot_event_id=event_id,
        event_time_ms=event_time_ms,
        created_at_ms=event_time_ms + 1,
        protective_order_confirmed=protective,
        uncertainty_state=uncertainty,  # type: ignore[arg-type]
        release_event_id=release_event_id,
    )


def _adopt(repository: GuardianRepository) -> None:
    assert repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )


def _single_pending_payload(repository: GuardianRepository) -> dict[str, object]:
    outbox = repository.list_guardian_alert_outbox()
    assert len(outbox) == 1
    assert outbox[0].status == "disabled"
    payload = json.loads(outbox[0].payload_json)
    assert isinstance(payload, dict)
    return payload


def _seed_pending_transport_row(
    repository: GuardianRepository,
    *,
    alert_id: str,
    source_event_id: str,
    created_at_ms: int,
) -> None:
    payload_json = json.dumps(
        {"schema_version": "test", "alert_id": alert_id, "code": "TEST"},
        separators=(",", ":"),
        sort_keys=True,
    )
    payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
    with Session(repository._engine) as session, session.begin():  # type: ignore[attr-defined]
        session.add(
            GuardianAlertOutboxRow(
                alert_id=alert_id,
                source_event_id=source_event_id,
                payload_json=payload_json,
                payload_sha256=payload_sha256,
                status="pending",
                attempts=0,
                created_at_ms=created_at_ms,
                updated_at_ms=created_at_ms,
            )
        )


def test_manual_size_increase_survives_restart_without_recomputing_projection(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'guardian.db').as_posix()}"
    with GuardianRepository(database_url) as repository:
        _adopt(repository)
        first = reconcile_once(repository, _request(event_id="snapshot-add", amount="0.02"))
        assert first.alerts == ("MANUAL_POSITION_ADD",)
        payload = _single_pending_payload(repository)
        assert payload["code"] == "MANUAL_SIZE_INCREASE"
        alert_id = str(payload["alert_id"])

    with GuardianRepository(database_url) as restarted:
        recovery = recover_guardian_alerts(restarted, now_ms=9000)
        assert recovery.alerts_inserted == 0
        assert recovery.alerts_projected == 1
        outbox = restarted.list_guardian_alert_outbox()
        assert [item.alert_id for item in outbox] == [alert_id]
        assert outbox[0].status == "disabled"
        assert '"private-account-sentinel"' not in outbox[0].payload_json
        assert '"position_ref"' not in outbox[0].payload_json


def test_side_flip_alert_survives_release_and_restart(tmp_path: Path) -> None:
    database_url = f"sqlite:///{(tmp_path / 'guardian.db').as_posix()}"
    with GuardianRepository(database_url) as repository:
        _adopt(repository)
        first = reconcile_once(
            repository,
            _request(
                event_id="snapshot-flip",
                amount="-0.01",
                release_event_id="release-flip",
            ),
        )
        assert first.alerts == ("SIDE_FLIP_REQUIRES_REAPPROVAL",)
        payload = _single_pending_payload(repository)
        assert payload["code"] == "SIDE_FLIP"
        alert_id = str(payload["alert_id"])

    with GuardianRepository(database_url) as restarted:
        assert restarted.get_projection(_identity()).state == "RELEASED"  # type: ignore[union-attr]
        recovery = recover_guardian_alerts(restarted, now_ms=9000)
        assert recovery.alerts_inserted == 0
        assert restarted.get_guardian_alert_outbox(alert_id) is not None


@pytest.mark.parametrize(
    ("first_protective", "second_protective", "first_uncertainty", "second_uncertainty"),
    [
        (False, True, "CERTAIN", "CERTAIN"),
        (True, True, "DEGRADED", "CERTAIN"),
    ],
)
def test_same_snapshot_event_id_cannot_mutate_alert_driving_evidence(
    first_protective: bool,
    second_protective: bool,
    first_uncertainty: str,
    second_uncertainty: str,
) -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        reconcile_once(
            repository,
            _request(
                event_id="snapshot-fixed",
                protective=first_protective,
                uncertainty=first_uncertainty,
            ),
        )
        with pytest.raises(GuardianEventConflictError):
            reconcile_once(
                repository,
                _request(
                    event_id="snapshot-fixed",
                    protective=second_protective,
                    uncertainty=second_uncertainty,
                ),
            )


def test_same_source_event_id_with_changed_event_time_is_hard_conflict() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        reconcile_once(repository, _request(event_id="snapshot-time", event_time_ms=3000))
        with pytest.raises(GuardianEventConflictError):
            reconcile_once(repository, _request(event_id="snapshot-time", event_time_ms=3001))


def test_restart_quarantines_sending_without_reopening_or_retrying() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        source_id = "source-no-projection"
        repository.record_shadow_alert_source(
            event_id=source_id,
            event_time_ms=3000,
            created_at_ms=3001,
            identity=_identity(),
            payload={
                "schema_version": "non-alert-source-v1",
                "reason": "NONE",
                "delivery_mode": "discord_v1",
            },
        )
        alert_id = "a" * 64
        _seed_pending_transport_row(
            repository,
            alert_id=alert_id,
            source_event_id=source_id,
            created_at_ms=3001,
        )
        claimed = repository.claim_guardian_alert(alert_id, 4000)
        assert claimed is not None
        assert claimed.status == "sending"
        assert claimed.attempts == 1

        recovery = recover_guardian_alerts(repository, now_ms=5000)
        assert recovery.inflight_quarantined == 1
        assert recovery.alerts_inserted == 0
        assert repository.pending_guardian_alerts() == []
        item = repository.get_guardian_alert_outbox(alert_id)
        assert item is not None
        assert item.status == "uncertain"
        assert item.attempts == 1
        assert item.detail_code == "PROCESS_RESTART_IN_FLIGHT"


def test_delivered_alert_is_not_reopened_by_restart_recovery() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        source_id = "source-delivered"
        repository.record_shadow_alert_source(
            event_id=source_id,
            event_time_ms=3000,
            created_at_ms=3001,
            identity=_identity(),
            payload={
                "schema_version": "non-alert-source-v1",
                "reason": "NONE",
                "delivery_mode": "discord_v1",
            },
        )
        alert_id = "b" * 64
        _seed_pending_transport_row(
            repository,
            alert_id=alert_id,
            source_event_id=source_id,
            created_at_ms=3001,
        )
        assert repository.claim_guardian_alert(alert_id, 4000) is not None
        assert repository.mark_guardian_alert(
            alert_id,
            "delivered",
            4001,
            response_code=200,
            message_id="discord-message-1",
        )

        recovery = recover_guardian_alerts(repository, now_ms=5000)
        assert recovery.inflight_quarantined == 0
        assert recovery.alerts_inserted == 0
        delivered = repository.get_guardian_alert_outbox(alert_id)
        assert delivered is not None
        assert delivered.status == "delivered"
        assert delivered.attempts == 1


def test_alert_outbox_same_id_changed_payload_is_hard_conflict() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        reconcile_once(
            repository,
            _request(event_id="snapshot-missing", protective=False),
        )
        item = repository.list_guardian_alert_outbox()[0]
        payload = json.loads(item.payload_json)
        assert isinstance(payload, dict)
        payload["message"] = "changed"
        with pytest.raises(GuardianAlertOutboxConflictError):
            repository.enqueue_guardian_alert(
                alert_id=item.alert_id,
                source_event_id=item.source_event_id,
                payload=payload,
                created_at_ms=6000,
            )


def test_multi_alert_source_has_stable_sanitized_payloads_across_recovery() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        result = reconcile_once(
            repository,
            _request(event_id="snapshot-two-alerts", amount="0.02", protective=False),
        )
        assert result.alerts == ("MANUAL_POSITION_ADD", "PROTECTIVE_ORDER_MISSING")
        first = repository.list_guardian_alert_outbox()
        assert len(first) == 2
        assert all(item.status == "disabled" for item in first)
        first_payloads = {item.alert_id: item.payload_json for item in first}

        recovery = recover_guardian_alerts(repository, now_ms=7000)
        assert recovery.alerts_projected == 2
        assert recovery.alerts_inserted == 0
        second = repository.list_guardian_alert_outbox()
        assert {item.alert_id: item.payload_json for item in second} == first_payloads
        rendered = "".join(first_payloads.values())
        assert "private-account-sentinel" not in rendered
        assert "position_ref" not in rendered


def test_older_private_snapshot_cannot_regress_projection_and_is_durably_uncertain() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        first_request = ReconciliationRequest(
            identity=_identity(),
            position=_position(amount="0.02", update_time_ms=3000),
            snapshot_event_id="snapshot-newer",
            event_time_ms=4000,
            created_at_ms=4001,
            protective_order_confirmed=True,
        )
        first = reconcile_once(repository, first_request)
        assert first.state == "MANAGED_SHADOW"
        assert repository.get_projection(_identity()).quantity == "0.02"  # type: ignore[union-attr]

        stale_request = ReconciliationRequest(
            identity=_identity(),
            position=_position(amount="0.01", update_time_ms=2000),
            snapshot_event_id="snapshot-older",
            event_time_ms=5000,
            created_at_ms=5001,
            protective_order_confirmed=True,
        )
        stale = reconcile_once(repository, stale_request)

        assert stale.state == "DEGRADED"
        assert stale.alerts == ("RECONCILIATION_UNCERTAIN",)
        assert stale.snapshot_event_accepted is False
        assert repository.get_projection(_identity()).quantity == "0.02"  # type: ignore[union-attr]
        outbox = repository.list_guardian_alert_outbox()
        assert [json.loads(item.payload_json)["code"] for item in outbox] == [
            "MANUAL_SIZE_INCREASE",
            "RECONCILIATION_UNCERTAIN",
        ]


def test_same_private_snapshot_cursor_with_different_payload_is_durably_uncertain() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        first_request = ReconciliationRequest(
            identity=_identity(),
            position=_position(amount="0.01", update_time_ms=3000),
            snapshot_event_id="snapshot-first",
            event_time_ms=4000,
            created_at_ms=4001,
            protective_order_confirmed=True,
        )
        assert reconcile_once(repository, first_request).state == "MANAGED_SHADOW"

        conflict_request = ReconciliationRequest(
            identity=_identity(),
            position=_position(amount="0.02", update_time_ms=3000),
            snapshot_event_id="snapshot-conflict",
            event_time_ms=5000,
            created_at_ms=5001,
            protective_order_confirmed=True,
        )
        conflict = reconcile_once(repository, conflict_request)

        assert conflict.state == "DEGRADED"
        assert conflict.alerts == ("RECONCILIATION_UNCERTAIN",)
        assert repository.get_projection(_identity()).quantity == "0.01"  # type: ignore[union-attr]


def test_same_private_position_cursor_allows_new_mark_price_with_same_position_facts() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        first = PositionSnapshot(
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amount=Decimal("0.01"),
            entry_price=Decimal("60000"),
            mark_price=Decimal("61000"),
            unrealized_profit=Decimal("10"),
            update_time_ms=3000,
        )
        second = PositionSnapshot(
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amount=Decimal("0.01"),
            entry_price=Decimal("60000"),
            mark_price=Decimal("62000"),
            unrealized_profit=Decimal("20"),
            update_time_ms=3000,
        )
        assert reconcile_once(
            repository,
            ReconciliationRequest(
                identity=_identity(),
                position=first,
                snapshot_event_id="snapshot-mark-1",
                event_time_ms=4000,
                created_at_ms=4001,
                protective_order_confirmed=True,
            ),
        ).state == "MANAGED_SHADOW"
        assert reconcile_once(
            repository,
            ReconciliationRequest(
                identity=_identity(),
                position=second,
                snapshot_event_id="snapshot-mark-2",
                event_time_ms=5000,
                created_at_ms=5001,
                protective_order_confirmed=True,
            ),
        ).state == "MANAGED_SHADOW"
        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.mark_price == "62000"
        assert projection.highest_price_since_adoption == "62000"


def test_restart_rebuilds_missing_outbox_from_source_event_without_recomputation() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        source_event_id = "stale-source-before-outbox"
        assert repository.record_shadow_alert_source(
            event_id=source_event_id,
            event_time_ms=6000,
            created_at_ms=6001,
            identity=_identity(),
            payload={
                "schema_version": GUARDIAN_SHADOW_CONTEXT_ALERT_SOURCE_V1,
                "disposition": "CONTEXT_REJECTED",
                "reason": "STALE_CONTEXT",
            },
        )
        assert repository.list_guardian_alert_outbox() == []

        recovery = recover_guardian_alerts(repository, now_ms=9000)

        assert recovery.alerts_inserted == 1
        outbox = repository.list_guardian_alert_outbox()
        assert len(outbox) == 1
        assert outbox[0].status == "disabled"
        assert outbox[0].created_at_ms == 6001
        assert json.loads(outbox[0].payload_json)["code"] == "STALE_CONTEXT"


def test_first_post_adoption_snapshot_older_than_adoption_source_cannot_regress() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        stale_request = ReconciliationRequest(
            identity=_identity(),
            position=_position(amount="0.005", update_time_ms=1000),
            snapshot_event_id="snapshot-pre-adoption",
            event_time_ms=4000,
            created_at_ms=4001,
            protective_order_confirmed=True,
        )

        stale = reconcile_once(repository, stale_request)

        assert stale.state == "DEGRADED"
        assert stale.alerts == ("RECONCILIATION_UNCERTAIN",)
        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.state == "ADOPTED"
        assert projection.quantity == "0.01"


def test_terminal_snapshot_and_release_are_atomic_before_outbox_recovery() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        assert repository.record_account_snapshot(
            event_id="snapshot-atomic-flip",
            event_time_ms=4000,
            created_at_ms=4001,
            identity=_identity(),
            position=_position(amount="-0.01", update_time_ms=3000),
            protective_order_confirmed=True,
            uncertainty_state="CERTAIN",
            shadow_mode=True,
            terminal_release_event_id="release-atomic-flip",
            terminal_release_reason="SIDE_FLIP_REQUIRES_REAPPROVAL",
        )
        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.state == "RELEASED"
        assert repository.list_guardian_alert_outbox() == []
        assert [event.event_type for event in repository.list_events(identity=_identity())] == [
            "ADOPTION",
            "ACCOUNT_SNAPSHOT",
            "RELEASE",
        ]

        recovery = recover_guardian_alerts(repository, now_ms=9000)

        assert recovery.alerts_inserted == 1
        outbox = repository.list_guardian_alert_outbox()
        assert len(outbox) == 1
        assert json.loads(outbox[0].payload_json)["code"] == "SIDE_FLIP"


def test_terminal_snapshot_replay_cannot_change_release_identity_or_reason() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        kwargs = {
            "event_id": "snapshot-terminal-fixed",
            "event_time_ms": 4000,
            "created_at_ms": 4001,
            "identity": _identity(),
            "position": _position(amount="-0.01", update_time_ms=3000),
            "protective_order_confirmed": True,
            "uncertainty_state": "CERTAIN",
            "shadow_mode": True,
            "terminal_release_event_id": "release-terminal-a",
            "terminal_release_reason": "SIDE_FLIP_REQUIRES_REAPPROVAL",
        }
        assert repository.record_account_snapshot(**kwargs)

        with pytest.raises(GuardianEventConflictError):
            repository.record_account_snapshot(
                **{**kwargs, "terminal_release_event_id": "release-terminal-b"}
            )
        with pytest.raises(GuardianEventConflictError):
            repository.record_account_snapshot(
                **{**kwargs, "terminal_release_reason": "POSITION_CLOSED"}
            )

        assert repository.get_event("release-terminal-b") is None
        releases = [
            event
            for event in repository.list_events(identity=_identity())
            if event.event_type == "RELEASE"
        ]
        assert [event.event_id for event in releases] == ["release-terminal-a"]


def test_repository_rejects_semantically_mismatched_terminal_release_reason() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)

        with pytest.raises(ValueError, match="POSITION_CLOSED requires zero"):
            repository.record_account_snapshot(
                event_id="snapshot-bad-close",
                event_time_ms=4000,
                created_at_ms=4001,
                identity=_identity(),
                position=_position(amount="-0.01", update_time_ms=3000),
                protective_order_confirmed=True,
                uncertainty_state="CERTAIN",
                shadow_mode=True,
                terminal_release_event_id="release-bad-close",
                terminal_release_reason="POSITION_CLOSED",
            )

        with pytest.raises(ValueError, match="requires an observed side flip"):
            repository.record_account_snapshot(
                event_id="snapshot-bad-flip",
                event_time_ms=5000,
                created_at_ms=5001,
                identity=_identity(),
                position=_position(amount="0.01", update_time_ms=3000),
                protective_order_confirmed=True,
                uncertainty_state="CERTAIN",
                shadow_mode=True,
                terminal_release_event_id="release-bad-flip",
                terminal_release_reason="SIDE_FLIP_REQUIRES_REAPPROVAL",
            )

        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.state == "ADOPTED"
        assert all(
            event.event_type != "RELEASE"
            for event in repository.list_events(identity=_identity())
        )


def test_full_close_persists_closed_state_across_restart(tmp_path: Path) -> None:
    database_url = f"sqlite:///{(tmp_path / 'closed.db').as_posix()}"
    request = _request(
        event_id="snapshot-close",
        amount="0",
        release_event_id="release-close",
        event_time_ms=4000,
    )
    with GuardianRepository(database_url) as repository:
        _adopt(repository)
        first = reconcile_once(repository, request)
        assert first.state == "CLOSED"
        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.state == "CLOSED"

    with GuardianRepository(database_url) as restarted:
        recovery = recover_guardian_alerts(restarted, now_ms=9000)
        assert recovery.exchange_write_calls == 0
        projection = restarted.get_projection(_identity())
        assert projection is not None
        assert projection.state == "CLOSED"
        replay = reconcile_once(restarted, request)
        assert replay.state == "CLOSED"
        assert replay.alerts == ("IDENTITY_CLOSED",)


def test_terminal_snapshot_transaction_rolls_back_if_release_event_conflicts() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        assert repository.record_reconciliation_alert_source(
            event_id="release-conflict",
            event_time_ms=3500,
            created_at_ms=3501,
            identity=_identity(),
            payload={"schema_version": "test-conflict", "reason": "NONE"},
        )

        with pytest.raises(GuardianEventConflictError):
            repository.record_account_snapshot(
                event_id="snapshot-must-rollback",
                event_time_ms=4000,
                created_at_ms=4001,
                identity=_identity(),
                position=_position(amount="-0.01", update_time_ms=3000),
                protective_order_confirmed=True,
                uncertainty_state="CERTAIN",
                shadow_mode=True,
                terminal_release_event_id="release-conflict",
                terminal_release_reason="SIDE_FLIP_REQUIRES_REAPPROVAL",
            )

        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.state == "ADOPTED"
        assert projection.quantity == "0.01"
        assert repository.get_event("snapshot-must-rollback") is None


def test_disabled_guardian_alert_cannot_be_reactivated_without_new_contract() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        reconcile_once(
            repository,
            _request(event_id="snapshot-disabled", protective=False),
        )
        item = repository.list_guardian_alert_outbox()[0]
        assert item.status == "disabled"

        with pytest.raises(ValueError, match="only from sending"):
            repository.mark_guardian_alert(
                item.alert_id,
                "pending",
                5000,
                expected_status="disabled",
            )
        assert repository.claim_guardian_alert(item.alert_id, 5001) is None
        assert repository.get_guardian_alert_outbox(item.alert_id).status == "disabled"  # type: ignore[union-attr]


def test_legacy_active_projection_without_private_cursor_fails_closed() -> None:
    with GuardianRepository("sqlite://") as repository:
        _adopt(repository)
        with Session(repository._engine) as session, session.begin():  # type: ignore[attr-defined]
            session.execute(
                delete(PrivateSnapshotCursorRow).where(
                    PrivateSnapshotCursorRow.identity_key
                    == "private-account-sentinel|BTCUSDT|LONG|3"
                )
            )

        result = reconcile_once(
            repository,
            _request(event_id="snapshot-legacy-no-cursor", amount="0.02"),
        )

        assert result.state == "DEGRADED"
        assert result.alerts == ("RECONCILIATION_UNCERTAIN",)
        assert result.snapshot_event_accepted is False
        projection = repository.get_projection(_identity())
        assert projection is not None
        assert projection.quantity == "0.01"
        outbox = repository.list_guardian_alert_outbox()
        assert len(outbox) == 1
        assert json.loads(outbox[0].payload_json)["code"] == "RECONCILIATION_UNCERTAIN"
