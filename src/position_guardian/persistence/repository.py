from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import create_engine, func, select, update
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from position_guardian.alert_contract import (
    GUARDIAN_ALERT_OUTBOX_ACTIVE_STATUSES,
    GUARDIAN_ALERT_SOURCE_EVENT_TYPES,
    GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1,
)
from position_guardian.domain import AdoptionCandidate, ManagedPositionIdentity
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.models import (
    GuardianAlertOutboxRow,
    GuardianBase,
    GuardianEventRow,
    ManagedPositionProjectionRow,
    PrivateSnapshotCursorRow,
    ReconciliationCursorRow,
)


class GuardianEventConflictError(RuntimeError):
    """Raised when an existing event ID is reused with different content."""


class GuardianProjectionError(RuntimeError):
    """Raised when a projection transition has no valid current state."""


class GuardianSnapshotOrderError(RuntimeError):
    """Raised when private position snapshot time would regress durable state."""


class GuardianAlertOutboxConflictError(RuntimeError):
    """Raised when one deterministic Guardian alert ID is reused with different content."""


class GuardianAlertOutboxCapacityError(RuntimeError):
    """Raised before enqueue when the active Guardian alert outbox is full."""


@dataclass(frozen=True, slots=True)
class GuardianAlertOutboxItem:
    alert_id: str
    source_event_id: str
    payload_json: str
    payload_sha256: str
    status: str
    attempts: int
    created_at_ms: int
    updated_at_ms: int
    response_code: int | None
    message_id: str | None
    detail_code: str | None


def _canonical_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"unsupported event payload value: {type(value).__name__}")


def _canonical_json(payload: Mapping[str, object]) -> tuple[str, str]:
    payload_json = json.dumps(
        _canonical_value(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return payload_json, hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _identity_key(identity: ManagedPositionIdentity) -> str:
    return "|".join(
        (
            identity.account_alias,
            identity.symbol,
            identity.position_side,
            str(identity.adoption_generation),
        )
    )


def _identity_payload(identity: ManagedPositionIdentity) -> dict[str, object]:
    return {
        "account_alias": identity.account_alias,
        "symbol": identity.symbol,
        "position_side": identity.position_side,
        "adoption_generation": identity.adoption_generation,
    }


def _candidate_payload(candidate: AdoptionCandidate) -> dict[str, object]:
    protective_order = candidate.protective_order
    return {
        "identity": _identity_payload(candidate.identity),
        "quantity": candidate.quantity,
        "entry_price": candidate.entry_price,
        "mark_price": candidate.mark_price,
        "source_update_time_ms": candidate.source_update_time_ms,
        "original_risk_stop": candidate.original_risk_stop,
        "protection_floor": candidate.protection_floor,
        "protection_source": candidate.protection_source,
        "protective_order": (
            {
                "source": protective_order.source,
                "order_id": protective_order.order_id,
                "trigger_price": protective_order.trigger_price,
            }
            if protective_order
            else None
        ),
    }


def _candidate_cursor_payload(candidate: AdoptionCandidate) -> dict[str, object]:
    return {
        "symbol": candidate.identity.symbol,
        "managed_side": candidate.identity.position_side,
        "quantity": candidate.quantity,
        "entry_price": candidate.entry_price,
        "update_time_ms": candidate.source_update_time_ms,
    }


def _position_payload(position: PositionSnapshot) -> dict[str, object]:
    return {
        "symbol": position.symbol,
        "position_side": position.position_side,
        "position_amount": position.position_amount,
        "entry_price": position.entry_price,
        "mark_price": position.mark_price,
        "unrealized_profit": position.unrealized_profit,
        "update_time_ms": position.update_time_ms,
    }


def _position_cursor_payload(position: PositionSnapshot) -> dict[str, object]:
    """Fields whose mutation must advance Binance private position updateTime."""

    return {
        "symbol": position.symbol,
        "managed_side": _logical_position_side(position),
        "quantity": abs(position.position_amount),
        "entry_price": position.entry_price,
        "update_time_ms": position.update_time_ms,
    }


def _logical_position_side(position: PositionSnapshot) -> str:
    if position.position_amount == 0:
        return "FLAT"
    if position.position_side == "BOTH":
        return "LONG" if position.position_amount > 0 else "SHORT"
    if position.position_side in {"LONG", "SHORT"} and position.position_amount > 0:
        return position.position_side
    return "INVALID"


def _outbox_item(row: GuardianAlertOutboxRow) -> GuardianAlertOutboxItem:
    return GuardianAlertOutboxItem(
        alert_id=row.alert_id,
        source_event_id=row.source_event_id,
        payload_json=row.payload_json,
        payload_sha256=row.payload_sha256,
        status=row.status,
        attempts=row.attempts,
        created_at_ms=row.created_at_ms,
        updated_at_ms=row.updated_at_ms,
        response_code=row.response_code,
        message_id=row.message_id,
        detail_code=row.detail_code,
    )


def _causal_event_order(rows: list[GuardianEventRow]) -> list[GuardianEventRow]:
    """Order atomic terminal snapshots before their same-time RELEASE children."""

    parent_snapshot_ids: set[str] = set()
    release_child_ids: set[str] = set()
    for row in rows:
        if row.event_type != "RELEASE":
            continue
        try:
            payload = json.loads(row.payload_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        parent = payload.get("parent_snapshot_event_id")
        if isinstance(parent, str) and parent:
            parent_snapshot_ids.add(parent)
            release_child_ids.add(row.event_id)

    def _key(row: GuardianEventRow) -> tuple[int, int, str]:
        if row.event_id in parent_snapshot_ids:
            priority = 0
        elif row.event_id in release_child_ids:
            priority = 2
        else:
            priority = 1
        return (row.event_time_ms, priority, row.event_id)

    return sorted(rows, key=_key)


def _validate_detail_code(detail_code: str | None) -> None:
    if detail_code is None:
        return
    if not detail_code or len(detail_code) > 64:
        raise ValueError("detail_code must be 1..64 characters")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in detail_code):
        raise ValueError("detail_code must be a sanitized uppercase code")


class GuardianRepository:
    """SQLite/PostgreSQL repository for the Guardian event ledger and projections."""

    def __init__(self, database_url: str, *, engine: Engine | None = None) -> None:
        if engine is not None:
            self._engine = engine
        else:
            engine_kwargs: dict[str, Any] = {}
            if database_url in {"sqlite://", "sqlite:///:memory:"}:
                engine_kwargs["connect_args"] = {"check_same_thread": False}
                engine_kwargs["poolclass"] = StaticPool
            self._engine = create_engine(database_url, **engine_kwargs)
        GuardianBase.metadata.create_all(self._engine)

    def close(self) -> None:
        self._engine.dispose()

    def __enter__(self) -> GuardianRepository:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def record_adoption(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        candidate: AdoptionCandidate,
    ) -> bool:
        identity_key = _identity_key(candidate.identity)
        payload = _candidate_payload(candidate)
        with Session(self._engine) as session, session.begin():
            appended = self._append_event(
                session,
                event_id=event_id,
                event_type="ADOPTION",
                identity_key=identity_key,
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload=payload,
            )
            if not appended:
                return False
            if session.get(ManagedPositionProjectionRow, identity_key) is not None:
                raise GuardianProjectionError("managed position identity is already projected")
            _cursor_json, cursor_sha256 = _canonical_json(_candidate_cursor_payload(candidate))
            private_cursor = session.get(PrivateSnapshotCursorRow, identity_key)
            if private_cursor is not None:
                if candidate.source_update_time_ms < private_cursor.update_time_ms:
                    raise GuardianSnapshotOrderError(
                        "adoption source update_time_ms regressed behind observed private state"
                    )
                if (
                    candidate.source_update_time_ms == private_cursor.update_time_ms
                    and cursor_sha256 != private_cursor.position_sha256
                ):
                    raise GuardianSnapshotOrderError(
                        "adoption source conflicts with observed private state at the same "
                        "update_time_ms"
                    )
            protective_order = candidate.protective_order
            session.add(
                ManagedPositionProjectionRow(
                    identity_key=identity_key,
                    account_alias=candidate.identity.account_alias,
                    symbol=candidate.identity.symbol,
                    position_side=candidate.identity.position_side,
                    adoption_generation=candidate.identity.adoption_generation,
                    state="ADOPTED",
                    quantity=str(candidate.quantity),
                    entry_price=str(candidate.entry_price),
                    mark_price=str(candidate.mark_price),
                    original_risk_stop=(
                        str(candidate.original_risk_stop)
                        if candidate.original_risk_stop is not None
                        else None
                    ),
                    protection_floor=str(candidate.protection_floor),
                    highest_price_since_adoption=str(candidate.mark_price),
                    lowest_price_since_adoption=str(candidate.mark_price),
                    active_protection_source=(
                        protective_order.source if protective_order is not None else "user_floor"
                    ),
                    active_protection_order_id=(
                        protective_order.order_id if protective_order is not None else None
                    ),
                    active_protection_trigger_price=(
                        str(protective_order.trigger_price)
                        if protective_order is not None
                        else None
                    ),
                    last_event_id=event_id,
                    updated_at_ms=created_at_ms,
                )
            )
            self._upsert_private_snapshot_cursor(
                session,
                identity_key=identity_key,
                update_time_ms=candidate.source_update_time_ms,
                position_sha256=cursor_sha256,
                event_id=event_id,
                created_at_ms=created_at_ms,
            )
            return True

    def record_account_snapshot(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        position: PositionSnapshot,
        protective_order_confirmed: bool | None = None,
        uncertainty_state: str | None = None,
        shadow_mode: bool | None = None,
        terminal_release_event_id: str | None = None,
        terminal_release_reason: str | None = None,
    ) -> bool:
        if (terminal_release_event_id is None) != (terminal_release_reason is None):
            raise ValueError("terminal release event ID and reason must be supplied together")
        evidence_values = (
            protective_order_confirmed,
            uncertainty_state,
            shadow_mode,
        )
        evidence_enabled = any(value is not None for value in evidence_values)
        if evidence_enabled and any(value is None for value in evidence_values):
            raise ValueError("reconciliation alert-source evidence must be complete")
        if uncertainty_state is not None and uncertainty_state not in {"CERTAIN", "DEGRADED"}:
            raise ValueError("unsupported reconciliation uncertainty_state")

        identity_key = _identity_key(identity)
        with Session(self._engine) as session, session.begin():
            existing = session.get(GuardianEventRow, event_id)
            if existing is not None:
                self._validate_account_snapshot_replay(
                    existing=existing,
                    identity_key=identity_key,
                    event_time_ms=event_time_ms,
                    identity=identity,
                    position=position,
                    protective_order_confirmed=protective_order_confirmed,
                    uncertainty_state=uncertainty_state,
                    shadow_mode=shadow_mode,
                    terminal_release_event_id=terminal_release_event_id,
                    terminal_release_reason=terminal_release_reason,
                )
                if terminal_release_event_id is not None and terminal_release_reason is not None:
                    self._apply_terminal_release(
                        session,
                        identity_key=identity_key,
                        identity=identity,
                        event_id=terminal_release_event_id,
                        event_time_ms=event_time_ms,
                        created_at_ms=created_at_ms,
                        reason=terminal_release_reason,
                        parent_snapshot_event_id=event_id,
                    )
                return False

            if terminal_release_reason is not None and terminal_release_reason not in {
                "POSITION_CLOSED",
                "SIDE_FLIP_REQUIRES_REAPPROVAL",
            }:
                raise ValueError("unsupported terminal release reason")
            if terminal_release_reason is not None and uncertainty_state != "CERTAIN":
                raise ValueError("terminal release requires CERTAIN reconciliation evidence")
            if terminal_release_reason == "POSITION_CLOSED" and position.position_amount != 0:
                raise ValueError("POSITION_CLOSED requires zero position quantity")
            if terminal_release_reason == "SIDE_FLIP_REQUIRES_REAPPROVAL":
                actual_side = _logical_position_side(position)
                if actual_side not in {"LONG", "SHORT"} or actual_side == identity.position_side:
                    raise ValueError(
                        "SIDE_FLIP_REQUIRES_REAPPROVAL requires an observed side flip"
                    )

            position_payload = _position_payload(position)
            _position_json, position_sha256 = _canonical_json(_position_cursor_payload(position))
            projection = session.get(ManagedPositionProjectionRow, identity_key)
            if projection is None or projection.state not in {"RELEASED", "CLOSED"}:
                private_cursor = session.get(PrivateSnapshotCursorRow, identity_key)
                if projection is not None and private_cursor is None:
                    raise GuardianSnapshotOrderError(
                        "private position snapshot cursor is missing for active projection"
                    )
                if private_cursor is not None:
                    if position.update_time_ms < private_cursor.update_time_ms:
                        raise GuardianSnapshotOrderError(
                            "private position snapshot update_time_ms regressed"
                        )
                    if (
                        position.update_time_ms == private_cursor.update_time_ms
                        and position_sha256 != private_cursor.position_sha256
                    ):
                        raise GuardianSnapshotOrderError(
                            "private position snapshot conflicts at the same update_time_ms"
                        )
            payload: dict[str, object] = {
                "identity": _identity_payload(identity),
                "position": position_payload,
            }
            if evidence_enabled:
                payload["alert_source"] = {
                    "schema_version": GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1,
                    "protective_order_confirmed": cast(bool, protective_order_confirmed),
                    "uncertainty_state": cast(str, uncertainty_state),
                    "shadow_mode": cast(bool, shadow_mode),
                    "projection_state_before": projection.state if projection is not None else None,
                    "previous_quantity": projection.quantity if projection is not None else None,
                }
            if terminal_release_event_id is not None and terminal_release_reason is not None:
                payload["terminal_release"] = {
                    "event_id": terminal_release_event_id,
                    "reason": terminal_release_reason,
                }
            appended = self._append_event(
                session,
                event_id=event_id,
                event_type="ACCOUNT_SNAPSHOT",
                identity_key=identity_key,
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload=payload,
            )
            if not appended:
                return False
            if projection is None:
                self._upsert_private_snapshot_cursor(
                    session,
                    identity_key=identity_key,
                    update_time_ms=position.update_time_ms,
                    position_sha256=position_sha256,
                    event_id=event_id,
                    created_at_ms=created_at_ms,
                )
                return True
            if projection.state in {"RELEASED", "CLOSED"}:
                return True
            quantity = abs(position.position_amount)
            projection.quantity = str(quantity)
            projection.mark_price = str(position.mark_price)
            projection.highest_price_since_adoption = str(
                max(Decimal(projection.highest_price_since_adoption), position.mark_price)
            )
            projection.lowest_price_since_adoption = str(
                min(Decimal(projection.lowest_price_since_adoption), position.mark_price)
            )
            projection.last_event_id = event_id
            projection.updated_at_ms = created_at_ms
            self._upsert_private_snapshot_cursor(
                session,
                identity_key=identity_key,
                update_time_ms=position.update_time_ms,
                position_sha256=position_sha256,
                event_id=event_id,
                created_at_ms=created_at_ms,
            )
            if terminal_release_event_id is not None and terminal_release_reason is not None:
                self._apply_terminal_release(
                    session,
                    identity_key=identity_key,
                    identity=identity,
                    event_id=terminal_release_event_id,
                    event_time_ms=event_time_ms,
                    created_at_ms=created_at_ms,
                    reason=terminal_release_reason,
                    parent_snapshot_event_id=event_id,
                )
            return True

    def _apply_terminal_release(
        self,
        session: Session,
        *,
        identity_key: str,
        identity: ManagedPositionIdentity,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        reason: str,
        parent_snapshot_event_id: str,
    ) -> None:
        projection = session.get(ManagedPositionProjectionRow, identity_key)
        if projection is None:
            raise GuardianProjectionError("cannot release a position without an active projection")
        self._append_event(
            session,
            event_id=event_id,
            event_type="RELEASE",
            identity_key=identity_key,
            event_time_ms=event_time_ms,
            created_at_ms=created_at_ms,
            payload={
                "identity": _identity_payload(identity),
                "reason": reason,
                "parent_snapshot_event_id": parent_snapshot_event_id,
            },
        )
        if projection.state not in {"RELEASED", "CLOSED"}:
            projection.state = "CLOSED" if reason == "POSITION_CLOSED" else "RELEASED"
            projection.last_event_id = event_id
            projection.updated_at_ms = created_at_ms

    @staticmethod
    def _upsert_private_snapshot_cursor(
        session: Session,
        *,
        identity_key: str,
        update_time_ms: int,
        position_sha256: str,
        event_id: str,
        created_at_ms: int,
    ) -> None:
        cursor = session.get(PrivateSnapshotCursorRow, identity_key)
        if cursor is None:
            session.add(
                PrivateSnapshotCursorRow(
                    identity_key=identity_key,
                    update_time_ms=update_time_ms,
                    position_sha256=position_sha256,
                    last_event_id=event_id,
                    updated_at_ms=created_at_ms,
                )
            )
            return
        cursor.update_time_ms = update_time_ms
        cursor.position_sha256 = position_sha256
        cursor.last_event_id = event_id
        cursor.updated_at_ms = created_at_ms

    @staticmethod
    def _validate_account_snapshot_replay(
        *,
        existing: GuardianEventRow,
        identity_key: str,
        event_time_ms: int,
        identity: ManagedPositionIdentity,
        position: PositionSnapshot,
        protective_order_confirmed: bool | None,
        uncertainty_state: str | None,
        shadow_mode: bool | None,
        terminal_release_event_id: str | None,
        terminal_release_reason: str | None,
    ) -> None:
        if (
            existing.event_type != "ACCOUNT_SNAPSHOT"
            or existing.identity_key != identity_key
            or existing.event_time_ms != event_time_ms
        ):
            raise GuardianEventConflictError(
                f"event_id={existing.event_id} is already bound to different content"
            )
        try:
            payload = json.loads(existing.payload_json)
        except json.JSONDecodeError as exc:
            raise GuardianEventConflictError(
                f"event_id={existing.event_id} has invalid durable payload"
            ) from exc
        expected_identity = _canonical_value(_identity_payload(identity))
        expected_position = _canonical_value(_position_payload(position))
        expected_terminal_release = (
            {
                "event_id": terminal_release_event_id,
                "reason": terminal_release_reason,
            }
            if terminal_release_event_id is not None and terminal_release_reason is not None
            else None
        )
        if (
            not isinstance(payload, dict)
            or payload.get("identity") != expected_identity
            or payload.get("position") != expected_position
            or payload.get("terminal_release") != expected_terminal_release
        ):
            raise GuardianEventConflictError(
                f"event_id={existing.event_id} is already bound to different content"
            )
        alert_source = payload.get("alert_source")
        if alert_source is None:
            # Legacy pre-L60-06 snapshots are valid duplicates but cannot be promoted
            # into restart-authoritative alert evidence after the fact.
            return
        expected_request = {
            "schema_version": GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1,
            "protective_order_confirmed": protective_order_confirmed,
            "uncertainty_state": uncertainty_state,
            "shadow_mode": shadow_mode,
        }
        if not isinstance(alert_source, dict) or any(
            alert_source.get(key) != value for key, value in expected_request.items()
        ):
            raise GuardianEventConflictError(
                f"event_id={existing.event_id} is already bound to different "
                "reconciliation evidence"
            )

    def record_release(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        reason: str,
    ) -> bool:
        identity_key = _identity_key(identity)
        with Session(self._engine) as session, session.begin():
            projection = session.get(ManagedPositionProjectionRow, identity_key)
            if projection is None:
                raise GuardianProjectionError("cannot release an identity without a projection")
            appended = self._append_event(
                session,
                event_id=event_id,
                event_type="RELEASE",
                identity_key=identity_key,
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload={"identity": _identity_payload(identity), "reason": reason},
            )
            if not appended:
                return False
            if projection.state in {"RELEASED", "CLOSED"}:
                raise GuardianProjectionError("managed position identity is already terminal")
            projection.state = "RELEASED"
            projection.last_event_id = event_id
            projection.updated_at_ms = created_at_ms
            return True

    def record_planned_intent(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        intent_type: str,
        payload: Mapping[str, object],
    ) -> bool:
        return self._record_identity_event(
            event_id=event_id,
            event_type="PLANNED_INTENT",
            event_time_ms=event_time_ms,
            created_at_ms=created_at_ms,
            identity=identity,
            payload={
                "identity": _identity_payload(identity),
                "intent_type": intent_type,
                "payload": dict(payload),
            },
        )

    def record_shadow_alert_source(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        payload: Mapping[str, object],
    ) -> bool:
        """Persist one immutable shadow-only alert source for restart replay."""

        return self._record_identity_event(
            event_id=event_id,
            event_type="SHADOW_ALERT_SOURCE",
            event_time_ms=event_time_ms,
            created_at_ms=created_at_ms,
            identity=identity,
            payload={
                "identity": _identity_payload(identity),
                "payload": dict(payload),
            },
        )

    def record_reconciliation_alert_source(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        payload: Mapping[str, object],
    ) -> bool:
        """Persist a reconciliation rejection that intentionally mutated no projection."""

        return self._record_identity_event(
            event_id=event_id,
            event_type="RECONCILIATION_ALERT_SOURCE",
            event_time_ms=event_time_ms,
            created_at_ms=created_at_ms,
            identity=identity,
            payload={
                "identity": _identity_payload(identity),
                "payload": dict(payload),
            },
        )

    def record_execution_receipt(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        intent_id: str,
        outcome: str,
        payload: Mapping[str, object],
    ) -> bool:
        return self._record_identity_event(
            event_id=event_id,
            event_type="EXECUTION_RECEIPT",
            event_time_ms=event_time_ms,
            created_at_ms=created_at_ms,
            identity=identity,
            payload={
                "identity": _identity_payload(identity),
                "intent_id": intent_id,
                "outcome": outcome,
                "payload": dict(payload),
            },
        )

    def record_reconciliation_cursor(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        cursor_name: str,
        cursor_value: str,
        uncertainty_state: str,
        detail: str | None = None,
    ) -> bool:
        payload = {
            "cursor_name": cursor_name,
            "cursor_value": cursor_value,
            "uncertainty_state": uncertainty_state,
            "detail": detail,
        }
        with Session(self._engine) as session, session.begin():
            appended = self._append_event(
                session,
                event_id=event_id,
                event_type="RECONCILIATION_CURSOR",
                identity_key=None,
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload=payload,
            )
            if not appended:
                return False
            cursor = session.get(ReconciliationCursorRow, cursor_name)
            if cursor is None:
                cursor = ReconciliationCursorRow(
                    cursor_name=cursor_name,
                    cursor_value=cursor_value,
                    uncertainty_state=uncertainty_state,
                    detail=detail,
                    last_event_id=event_id,
                    updated_at_ms=created_at_ms,
                )
                session.add(cursor)
            else:
                cursor.cursor_value = cursor_value
                cursor.uncertainty_state = uncertainty_state
                cursor.detail = detail
                cursor.last_event_id = event_id
                cursor.updated_at_ms = created_at_ms
            return True

    def get_projection(
        self, identity: ManagedPositionIdentity
    ) -> ManagedPositionProjectionRow | None:
        with Session(self._engine) as session:
            return session.get(ManagedPositionProjectionRow, _identity_key(identity))

    def get_cursor(self, cursor_name: str) -> ReconciliationCursorRow | None:
        with Session(self._engine) as session:
            return session.get(ReconciliationCursorRow, cursor_name)

    def get_event(self, event_id: str) -> GuardianEventRow | None:
        with Session(self._engine) as session:
            return session.get(GuardianEventRow, event_id)

    def list_events(
        self, *, identity: ManagedPositionIdentity | None = None
    ) -> list[GuardianEventRow]:
        with Session(self._engine) as session:
            statement = select(GuardianEventRow)
            if identity is not None:
                statement = statement.where(
                    GuardianEventRow.identity_key == _identity_key(identity)
                )
            rows = list(session.scalars(statement).all())
            return _causal_event_order(rows)

    def list_alert_source_events(self) -> list[GuardianEventRow]:
        with Session(self._engine) as session:
            statement = (
                select(GuardianEventRow)
                .where(GuardianEventRow.event_type.in_(GUARDIAN_ALERT_SOURCE_EVENT_TYPES))
                .order_by(GuardianEventRow.event_time_ms, GuardianEventRow.event_id)
            )
            return list(session.scalars(statement).all())

    def enqueue_guardian_alert(
        self,
        *,
        alert_id: str,
        source_event_id: str,
        payload: Mapping[str, object],
        created_at_ms: int,
        maximum_active_items: int = 10_000,
    ) -> bool:
        if len(alert_id) != 64:
            raise ValueError("Guardian alert_id must be a 64-character SHA-256 hex digest")
        try:
            int(alert_id, 16)
        except ValueError as exc:
            raise ValueError("Guardian alert_id must be hexadecimal") from exc
        if maximum_active_items < 1:
            raise ValueError("maximum_active_items must be positive")
        payload_json, payload_sha256 = _canonical_json(payload)
        with Session(self._engine) as session, session.begin():
            source_event = session.get(GuardianEventRow, source_event_id)
            if source_event is None:
                raise GuardianProjectionError("Guardian alert source event does not exist")
            if source_event.event_type not in GUARDIAN_ALERT_SOURCE_EVENT_TYPES:
                raise GuardianProjectionError(
                    "Guardian alert source event type is not eligible for materialization"
                )
            existing = session.get(GuardianAlertOutboxRow, alert_id)
            if existing is not None:
                if (
                    existing.source_event_id != source_event_id
                    or existing.payload_sha256 != payload_sha256
                    or existing.payload_json != payload_json
                ):
                    raise GuardianAlertOutboxConflictError(
                        f"alert_id={alert_id} is already bound to different content"
                    )
                return False
            active = session.scalar(
                select(func.count())
                .select_from(GuardianAlertOutboxRow)
                .where(GuardianAlertOutboxRow.status.in_(GUARDIAN_ALERT_OUTBOX_ACTIVE_STATUSES))
            )
            if int(active or 0) >= maximum_active_items:
                raise GuardianAlertOutboxCapacityError(
                    "active Guardian alert outbox reached its configured hard limit"
                )
            session.add(
                GuardianAlertOutboxRow(
                    alert_id=alert_id,
                    source_event_id=source_event_id,
                    payload_json=payload_json,
                    payload_sha256=payload_sha256,
                    status="disabled",
                    attempts=0,
                    created_at_ms=created_at_ms,
                    updated_at_ms=created_at_ms,
                )
            )
            return True

    def get_guardian_alert_outbox(self, alert_id: str) -> GuardianAlertOutboxItem | None:
        with Session(self._engine) as session:
            row = session.get(GuardianAlertOutboxRow, alert_id)
            return None if row is None else _outbox_item(row)

    def list_guardian_alert_outbox(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[GuardianAlertOutboxItem]:
        if limit < 1:
            raise ValueError("outbox limit must be positive")
        with Session(self._engine) as session:
            statement = select(GuardianAlertOutboxRow)
            if status is not None:
                statement = statement.where(GuardianAlertOutboxRow.status == status)
            rows = session.scalars(
                statement.order_by(
                    GuardianAlertOutboxRow.created_at_ms,
                    GuardianAlertOutboxRow.alert_id,
                ).limit(limit)
            ).all()
            return [_outbox_item(row) for row in rows]

    def pending_guardian_alerts(self, limit: int = 100) -> list[GuardianAlertOutboxItem]:
        if limit < 1:
            raise ValueError("outbox limit must be positive")
        with Session(self._engine) as session:
            rows = session.scalars(
                select(GuardianAlertOutboxRow)
                .where(GuardianAlertOutboxRow.status == "pending")
                .order_by(
                    GuardianAlertOutboxRow.created_at_ms,
                    GuardianAlertOutboxRow.alert_id,
                )
                .limit(limit)
            ).all()
            return [_outbox_item(row) for row in rows]

    def claim_guardian_alert(
        self, alert_id: str, updated_at_ms: int
    ) -> GuardianAlertOutboxItem | None:
        with Session(self._engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(GuardianAlertOutboxRow)
                    .where(
                        GuardianAlertOutboxRow.alert_id == alert_id,
                        GuardianAlertOutboxRow.status == "pending",
                    )
                    .values(
                        status="sending",
                        attempts=GuardianAlertOutboxRow.attempts + 1,
                        updated_at_ms=updated_at_ms,
                        response_code=None,
                        message_id=None,
                        detail_code=None,
                    )
                ),
            )
            if result.rowcount != 1:
                session.rollback()
                return None
            session.commit()
            row = session.get(GuardianAlertOutboxRow, alert_id)
            return None if row is None else _outbox_item(row)

    def mark_guardian_alert(
        self,
        alert_id: str,
        status: str,
        updated_at_ms: int,
        *,
        response_code: int | None = None,
        message_id: str | None = None,
        detail_code: str | None = None,
        expected_status: str = "sending",
    ) -> bool:
        if expected_status != "sending":
            raise ValueError("Guardian alert transitions are allowed only from sending")
        allowed = {"pending", "delivered", "uncertain", "dead"}
        if status not in allowed:
            raise ValueError(f"unsupported Guardian alert outbox status: {status}")
        _validate_detail_code(detail_code)
        with Session(self._engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(GuardianAlertOutboxRow)
                    .where(
                        GuardianAlertOutboxRow.alert_id == alert_id,
                        GuardianAlertOutboxRow.status == expected_status,
                    )
                    .values(
                        status=status,
                        updated_at_ms=updated_at_ms,
                        response_code=response_code,
                        message_id=message_id,
                        detail_code=detail_code,
                    )
                ),
            )
            session.commit()
            return result.rowcount == 1

    def mark_inflight_guardian_alerts_uncertain(self, updated_at_ms: int) -> int:
        """Quarantine Guardian delivery attempts interrupted by process restart."""

        with Session(self._engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(GuardianAlertOutboxRow)
                    .where(GuardianAlertOutboxRow.status == "sending")
                    .values(
                        status="uncertain",
                        updated_at_ms=updated_at_ms,
                        detail_code="PROCESS_RESTART_IN_FLIGHT",
                    )
                ),
            )
            session.commit()
            return int(result.rowcount or 0)

    def _record_identity_event(
        self,
        *,
        event_id: str,
        event_type: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        payload: Mapping[str, object],
    ) -> bool:
        with Session(self._engine) as session, session.begin():
            if session.get(ManagedPositionProjectionRow, _identity_key(identity)) is None:
                raise GuardianProjectionError("identity event requires an adopted projection")
            return self._append_event(
                session,
                event_id=event_id,
                event_type=event_type,
                identity_key=_identity_key(identity),
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload=payload,
            )

    @staticmethod
    def _append_event(
        session: Session,
        *,
        event_id: str,
        event_type: str,
        identity_key: str | None,
        event_time_ms: int,
        created_at_ms: int,
        payload: Mapping[str, object],
    ) -> bool:
        payload_json, payload_sha256 = _canonical_json(payload)
        existing = session.get(GuardianEventRow, event_id)
        if existing is not None:
            if (
                existing.event_type != event_type
                or existing.identity_key != identity_key
                or existing.event_time_ms != event_time_ms
                or existing.payload_sha256 != payload_sha256
            ):
                raise GuardianEventConflictError(
                    f"event_id={event_id} is already bound to different content"
                )
            return False
        session.add(
            GuardianEventRow(
                event_id=event_id,
                event_type=event_type,
                identity_key=identity_key,
                event_time_ms=event_time_ms,
                payload_json=payload_json,
                payload_sha256=payload_sha256,
                created_at_ms=created_at_ms,
            )
        )
        session.flush()
        return True
