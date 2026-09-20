from __future__ import annotations

from sqlalchemy import BigInteger, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class GuardianBase(DeclarativeBase):
    pass


class GuardianEventRow(GuardianBase):
    """Append-only ledger row for every accepted Guardian observation or intent."""

    __tablename__ = "guardian_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(40), index=True)
    identity_key: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    event_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    created_at_ms: Mapped[int] = mapped_column(BigInteger)


class ManagedPositionProjectionRow(GuardianBase):
    """Current projection; its source of truth remains the append-only ledger."""

    __tablename__ = "guardian_managed_positions"

    identity_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    account_alias: Mapped[str] = mapped_column(String(80), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    position_side: Mapped[str] = mapped_column(String(8))
    adoption_generation: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(24), index=True)
    quantity: Mapped[str] = mapped_column(String(64))
    entry_price: Mapped[str] = mapped_column(String(64))
    mark_price: Mapped[str] = mapped_column(String(64))
    original_risk_stop: Mapped[str | None] = mapped_column(String(64), nullable=True)
    protection_floor: Mapped[str] = mapped_column(String(64))
    highest_price_since_adoption: Mapped[str] = mapped_column(String(64))
    lowest_price_since_adoption: Mapped[str] = mapped_column(String(64))
    active_protection_source: Mapped[str | None] = mapped_column(String(24), nullable=True)
    active_protection_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    active_protection_trigger_price: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    last_event_id: Mapped[str] = mapped_column(String(128))
    updated_at_ms: Mapped[int] = mapped_column(BigInteger)


class ReconciliationCursorRow(GuardianBase):
    """Mutable cursor projection for restart and uncertainty handling."""

    __tablename__ = "guardian_reconciliation_cursors"

    cursor_name: Mapped[str] = mapped_column(String(80), primary_key=True)
    cursor_value: Mapped[str] = mapped_column(String(256))
    uncertainty_state: Mapped[str] = mapped_column(String(24))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_event_id: Mapped[str] = mapped_column(String(128))
    updated_at_ms: Mapped[int] = mapped_column(BigInteger)
