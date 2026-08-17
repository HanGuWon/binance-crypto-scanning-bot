from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import create_engine, desc, func, select, update
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from signalbot.domain.models import Candle, SignalDecision
from signalbot.persistence.models import (
    AlertOutboxRow,
    AlertRow,
    Base,
    CandleRow,
    OutcomeRow,
    SignalRow,
)


class EventIdConflictError(RuntimeError):
    """Raised when one deterministic event ID maps to different content."""


class OutboxCapacityError(RuntimeError):
    """Raised before persisting a signal when active delivery intent is full."""


@dataclass(frozen=True, slots=True)
class OutboxItem:
    event_id: str
    payload_json: str
    payload_sha256: str
    status: str
    attempts: int
    created_at_ms: int
    updated_at_ms: int
    response_code: int | None = None
    message_id: str | None = None
    detail: str | None = None


def _signal_payload(decision: SignalDecision) -> str:
    return json.dumps(
        decision.model_dump(mode="json"), separators=(",", ":"), ensure_ascii=False
    )


def _outbox_item(row: AlertOutboxRow) -> OutboxItem:
    return OutboxItem(
        event_id=row.event_id,
        payload_json=row.payload_json,
        payload_sha256=row.payload_sha256,
        status=row.status,
        attempts=row.attempts,
        created_at_ms=row.created_at_ms,
        updated_at_ms=row.updated_at_ms,
        response_code=row.response_code,
        message_id=row.message_id,
        detail=row.detail,
    )


def _upsert_candle(session: Session, candle: Candle) -> bool:
    existing = session.scalar(
        select(CandleRow).where(
            CandleRow.market == candle.market.value,
            CandleRow.symbol == candle.symbol,
            CandleRow.interval == candle.interval,
            CandleRow.open_time_ms == candle.open_time_ms,
        )
    )
    values = {
        "close_time_ms": candle.close_time_ms,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "quote_volume": str(candle.quote_volume),
        "trade_count": candle.trade_count,
        "taker_buy_base_volume": str(candle.taker_buy_base_volume),
        "taker_buy_quote_volume": str(candle.taker_buy_quote_volume),
        "is_closed": candle.is_closed,
    }
    if existing is None:
        session.add(
            CandleRow(
                market=candle.market.value,
                symbol=candle.symbol,
                interval=candle.interval,
                open_time_ms=candle.open_time_ms,
                **values,
            )
        )
        return True
    for key, value in values.items():
        setattr(existing, key, value)
    return False


class SqlRepository:
    def __init__(self, url: str, echo: bool = False) -> None:
        self.url = url
        self.echo = echo
        self._engine: Engine | None = None
        self.ready = False

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("repository has not been initialized")
        return self._engine

    def initialize(self) -> None:
        if self.url.startswith("sqlite:///") and not self.url.endswith(":memory:"):
            Path(self.url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
        kwargs: dict[str, Any] = {"echo": self.echo, "pool_pre_ping": True}
        if self.url.endswith(":memory:"):
            kwargs.update(connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self._engine = create_engine(self.url, **kwargs)
        Base.metadata.create_all(self._engine)
        self.ready = True

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
        self.ready = False

    def save_candle(self, c: Candle) -> bool:
        with Session(self.engine) as session:
            inserted = _upsert_candle(session, c)
            session.commit()
            return inserted

    def save_candles(self, candles: list[Candle]) -> int:
        """Persist one bounded candle batch in a single transaction."""

        if not candles:
            return 0
        with Session(self.engine) as session:
            inserted = sum(_upsert_candle(session, candle) for candle in candles)
            session.commit()
        return inserted

    def save_signal(self, d: SignalDecision) -> bool:
        with Session(self.engine) as session:
            payload = _signal_payload(d)
            existing = session.get(SignalRow, d.event_id)
            if existing is not None:
                if existing.payload_json != payload:
                    raise EventIdConflictError(
                        f"event ID {d.event_id} maps to conflicting signal payloads"
                    )
                return False
            session.add(
                SignalRow(
                    event_id=d.event_id,
                    market=d.market.value,
                    symbol=d.symbol,
                    family=d.family.value,
                    stage=d.stage.value,
                    direction=d.direction.value,
                    timeframe=d.timeframe,
                    event_time_ms=d.event_time_ms,
                    score=d.score,
                    price=str(d.price),
                    invalidation=str(d.invalidation) if d.invalidation is not None else None,
                    rule_version=d.rule_version,
                    payload_json=payload,
                )
            )
            session.commit()
            return True

    def save_signal_and_enqueue(
        self,
        decision: SignalDecision,
        payload: dict[str, object],
        created_at_ms: int,
        *,
        delivery_enabled: bool,
        maximum_active_items: int | None = None,
    ) -> bool:
        """Atomically persist a decision and one immutable delivery intent.

        Returns ``True`` only when a new outbox item was created. Replaying the
        same event and byte-equivalent payload is a no-op. Reusing an event ID
        for different content is a hard conflict.
        """

        signal_payload = _signal_payload(decision)
        outbox_payload = json.dumps(
            payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False
        )
        fingerprint = hashlib.sha256(outbox_payload.encode()).hexdigest()
        with Session(self.engine) as session:
            signal = session.get(SignalRow, decision.event_id)
            if signal is not None and signal.payload_json != signal_payload:
                raise EventIdConflictError(
                    f"event ID {decision.event_id} maps to conflicting signal payloads"
                )
            outbox = session.get(AlertOutboxRow, decision.event_id)
            if outbox is not None:
                if outbox.payload_sha256 != fingerprint or outbox.payload_json != outbox_payload:
                    raise EventIdConflictError(
                        f"event ID {decision.event_id} maps to conflicting alert payloads"
                    )
                return False
            if delivery_enabled and maximum_active_items is not None:
                if maximum_active_items < 1:
                    raise ValueError("maximum_active_items must be positive")
                active = session.scalar(
                    select(func.count())
                    .select_from(AlertOutboxRow)
                    .where(
                        AlertOutboxRow.status.in_(
                            ("pending", "sending", "uncertain")
                        )
                    )
                )
                if int(active or 0) >= maximum_active_items:
                    raise OutboxCapacityError(
                        "active Discord outbox reached its configured hard limit"
                    )
            if signal is None:
                session.add(
                    SignalRow(
                        event_id=decision.event_id,
                        market=decision.market.value,
                        symbol=decision.symbol,
                        family=decision.family.value,
                        stage=decision.stage.value,
                        direction=decision.direction.value,
                        timeframe=decision.timeframe,
                        event_time_ms=decision.event_time_ms,
                        score=decision.score,
                        price=str(decision.price),
                        invalidation=(
                            str(decision.invalidation)
                            if decision.invalidation is not None
                            else None
                        ),
                        rule_version=decision.rule_version,
                        payload_json=signal_payload,
                    )
                )
            session.add(
                AlertOutboxRow(
                    event_id=decision.event_id,
                    payload_json=outbox_payload,
                    payload_sha256=fingerprint,
                    status="pending" if delivery_enabled else "disabled",
                    attempts=0,
                    created_at_ms=created_at_ms,
                    updated_at_ms=created_at_ms,
                )
            )
            session.commit()
            return True

    def get_outbox(self, event_id: str) -> OutboxItem | None:
        with Session(self.engine) as session:
            row = session.get(AlertOutboxRow, event_id)
            return None if row is None else _outbox_item(row)

    def pending_outbox(self, limit: int = 100) -> list[OutboxItem]:
        if limit < 1:
            raise ValueError("outbox limit must be positive")
        with Session(self.engine) as session:
            rows = session.scalars(
                select(AlertOutboxRow)
                .where(AlertOutboxRow.status == "pending")
                .order_by(AlertOutboxRow.created_at_ms, AlertOutboxRow.event_id)
                .limit(limit)
            ).all()
            return [_outbox_item(row) for row in rows]

    def claim_outbox(self, event_id: str, updated_at_ms: int) -> OutboxItem | None:
        """Atomically claim one pending item for a single delivery attempt."""

        with Session(self.engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(AlertOutboxRow)
                    .where(
                        AlertOutboxRow.event_id == event_id,
                        AlertOutboxRow.status == "pending",
                    )
                    .values(
                        status="sending",
                        attempts=AlertOutboxRow.attempts + 1,
                        updated_at_ms=updated_at_ms,
                        response_code=None,
                        message_id=None,
                        detail=None,
                    )
                ),
            )
            if result.rowcount != 1:
                session.rollback()
                return None
            session.commit()
            row = session.get(AlertOutboxRow, event_id)
            return None if row is None else _outbox_item(row)

    def mark_outbox(
        self,
        event_id: str,
        status: str,
        updated_at_ms: int,
        *,
        response_code: int | None = None,
        message_id: str | None = None,
        detail: str | None = None,
        expected_status: str = "sending",
    ) -> bool:
        allowed = {"pending", "delivered", "uncertain", "dead", "disabled"}
        if status not in allowed:
            raise ValueError(f"unsupported outbox status: {status}")
        with Session(self.engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(AlertOutboxRow)
                    .where(
                        AlertOutboxRow.event_id == event_id,
                        AlertOutboxRow.status == expected_status,
                    )
                    .values(
                        status=status,
                        updated_at_ms=updated_at_ms,
                        response_code=response_code,
                        message_id=message_id,
                        detail=detail,
                    )
                ),
            )
            session.commit()
            return result.rowcount == 1

    def mark_inflight_uncertain(self, updated_at_ms: int) -> int:
        """Quarantine delivery attempts whose process ended while in flight."""

        with Session(self.engine) as session:
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(AlertOutboxRow)
                    .where(AlertOutboxRow.status == "sending")
                    .values(
                        status="uncertain",
                        updated_at_ms=updated_at_ms,
                        detail="process restarted while Discord delivery was in flight",
                    )
                ),
            )
            session.commit()
            return int(result.rowcount or 0)

    def record_alert(
        self,
        event_id: str,
        attempt: int,
        status: str,
        created_at_ms: int,
        response_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        with Session(self.engine) as session:
            existing = session.scalar(
                select(AlertRow).where(AlertRow.event_id == event_id, AlertRow.attempt == attempt)
            )
            if existing is None:
                session.add(
                    AlertRow(
                        event_id=event_id,
                        attempt=attempt,
                        status=status,
                        response_code=response_code,
                        detail=detail,
                        created_at_ms=created_at_ms,
                    )
                )
            else:
                existing.status = status
                existing.response_code = response_code
                existing.detail = detail
                existing.created_at_ms = created_at_ms
            session.commit()

    def has_successful_alert(self, event_id: str) -> bool:
        with Session(self.engine) as session:
            return (
                session.scalar(
                    select(AlertRow.id).where(
                        AlertRow.event_id == event_id, AlertRow.status == "sent"
                    )
                )
                is not None
            )

    def recent_signals(self, limit: int = 100) -> list[SignalDecision]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(SignalRow).order_by(desc(SignalRow.event_time_ms)).limit(limit)
            ).all()
        return [SignalDecision.model_validate(json.loads(row.payload_json)) for row in rows]

    def save_outcome(
        self,
        event_id: str,
        horizon_seconds: int,
        mfe: float,
        mae: float,
        close_return: float,
        observed_until_ms: int,
    ) -> None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(OutcomeRow).where(
                    OutcomeRow.event_id == event_id, OutcomeRow.horizon_seconds == horizon_seconds
                )
            )
            if row is None:
                session.add(
                    OutcomeRow(
                        event_id=event_id,
                        horizon_seconds=horizon_seconds,
                        mfe=mfe,
                        mae=mae,
                        close_return=close_return,
                        observed_until_ms=observed_until_ms,
                    )
                )
            else:
                row.mfe = mfe
                row.mae = mae
                row.close_return = close_return
                row.observed_until_ms = observed_until_ms
            session.commit()
