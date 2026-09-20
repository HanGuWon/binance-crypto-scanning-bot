from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from position_guardian.domain import AdoptionCandidate, ManagedPositionIdentity
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.models import (
    GuardianBase,
    GuardianEventRow,
    ManagedPositionProjectionRow,
    ReconciliationCursorRow,
)


class GuardianEventConflictError(RuntimeError):
    """Raised when an existing event ID is reused with different content."""


class GuardianProjectionError(RuntimeError):
    """Raised when a projection transition has no valid current state."""


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
            return True

    def record_account_snapshot(
        self,
        *,
        event_id: str,
        event_time_ms: int,
        created_at_ms: int,
        identity: ManagedPositionIdentity,
        position: PositionSnapshot,
    ) -> bool:
        identity_key = _identity_key(identity)
        with Session(self._engine) as session, session.begin():
            appended = self._append_event(
                session,
                event_id=event_id,
                event_type="ACCOUNT_SNAPSHOT",
                identity_key=identity_key,
                event_time_ms=event_time_ms,
                created_at_ms=created_at_ms,
                payload={
                    "identity": _identity_payload(identity),
                    "position": _position_payload(position),
                },
            )
            if not appended:
                return False
            projection = session.get(ManagedPositionProjectionRow, identity_key)
            if projection is None:
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
            return True

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

    def list_events(
        self, *, identity: ManagedPositionIdentity | None = None
    ) -> list[GuardianEventRow]:
        with Session(self._engine) as session:
            statement = select(GuardianEventRow).order_by(GuardianEventRow.event_time_ms)
            if identity is not None:
                statement = statement.where(
                    GuardianEventRow.identity_key == _identity_key(identity)
                )
            return list(session.scalars(statement).all())

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
