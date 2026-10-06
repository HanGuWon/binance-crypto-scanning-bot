from __future__ import annotations

from dataclasses import dataclass

from position_guardian.alerts import GuardianAlert, alerts_from_durable_event
from position_guardian.persistence.repository import GuardianRepository


@dataclass(frozen=True, slots=True)
class GuardianAlertMaterialization:
    alerts: tuple[GuardianAlert, ...]
    inserted_count: int


@dataclass(frozen=True, slots=True)
class GuardianAlertRecovery:
    source_events_scanned: int
    alerts_projected: int
    alerts_inserted: int
    inflight_quarantined: int
    unauthorized_pending_disabled: int = 0
    exchange_write_calls: int = 0


def materialize_guardian_alerts_for_source_event(
    repository: GuardianRepository,
    *,
    source_event_id: str,
    maximum_active_items: int = 10_000,
) -> GuardianAlertMaterialization:
    """Project one durable source event into idempotent sanitized outbox intents."""

    event = repository.get_event(source_event_id)
    if event is None:
        raise ValueError("Guardian alert source event does not exist")
    alerts = alerts_from_durable_event(
        event_type=event.event_type,
        payload_json=event.payload_json,
        source_event_id=event.event_id,
        event_time_ms=event.event_time_ms,
    )
    inserted = 0
    for alert in alerts:
        if alert.exchange_write_calls != 0:
            raise ValueError("Guardian alert materialization requires zero exchange writes")
        if repository.enqueue_guardian_alert(
            alert_id=alert.alert_id,
            source_event_id=event.event_id,
            payload=alert.as_dict(),
            created_at_ms=event.created_at_ms,
            maximum_active_items=maximum_active_items,
        ):
            inserted += 1
    return GuardianAlertMaterialization(alerts=alerts, inserted_count=inserted)


def recover_guardian_alerts(
    repository: GuardianRepository,
    *,
    now_ms: int,
    maximum_active_items: int = 10_000,
) -> GuardianAlertRecovery:
    """Restart recovery: quarantine in-flight sends, then rebuild missing alert intents."""

    if now_ms < 0:
        raise ValueError("now_ms must be non-negative")
    quarantined = repository.mark_inflight_guardian_alerts_uncertain(now_ms)
    unauthorized_pending_disabled = repository.disable_unauthorized_pending_guardian_alerts(now_ms)
    source_events = repository.list_alert_source_events()
    projected = 0
    inserted = 0
    for event in source_events:
        materialized = materialize_guardian_alerts_for_source_event(
            repository,
            source_event_id=event.event_id,
            maximum_active_items=maximum_active_items,
        )
        projected += len(materialized.alerts)
        inserted += materialized.inserted_count
    return GuardianAlertRecovery(
        source_events_scanned=len(source_events),
        alerts_projected=projected,
        alerts_inserted=inserted,
        inflight_quarantined=quarantined,
        unauthorized_pending_disabled=unauthorized_pending_disabled,
    )
