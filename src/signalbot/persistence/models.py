from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CandleRow(Base):
    __tablename__ = "candles"
    __table_args__ = (
        UniqueConstraint("market", "symbol", "interval", "open_time_ms", name="uq_candle"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String(16), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    interval: Mapped[str] = mapped_column(String(8), index=True)
    open_time_ms: Mapped[int] = mapped_column(BigInteger)
    close_time_ms: Mapped[int] = mapped_column(BigInteger)
    open: Mapped[str] = mapped_column(String(64))
    high: Mapped[str] = mapped_column(String(64))
    low: Mapped[str] = mapped_column(String(64))
    close: Mapped[str] = mapped_column(String(64))
    volume: Mapped[str] = mapped_column(String(64))
    quote_volume: Mapped[str] = mapped_column(String(64))
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    taker_buy_base_volume: Mapped[str] = mapped_column(String(64), default="0")
    taker_buy_quote_volume: Mapped[str] = mapped_column(String(64), default="0")
    is_closed: Mapped[bool] = mapped_column(Boolean, default=True)


class SignalRow(Base):
    __tablename__ = "signals"
    event_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    market: Mapped[str] = mapped_column(String(16), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    family: Mapped[str] = mapped_column(String(32), index=True)
    stage: Mapped[str] = mapped_column(String(16), index=True)
    direction: Mapped[str] = mapped_column(String(16))
    timeframe: Mapped[str] = mapped_column(String(16))
    event_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    score: Mapped[int] = mapped_column(Integer)
    price: Mapped[str] = mapped_column(String(64))
    invalidation: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rule_version: Mapped[str] = mapped_column(String(32))
    payload_json: Mapped[str] = mapped_column(Text)


class AlertRow(Base):
    __tablename__ = "alerts"
    __table_args__ = (UniqueConstraint("event_id", "attempt", name="uq_alert_event_attempt"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(32), index=True)
    attempt: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24), index=True)
    response_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_ms: Mapped[int] = mapped_column(BigInteger)


class AlertOutboxRow(Base):
    """Durable Discord delivery intent stored with its signal decision."""

    __tablename__ = "alert_outbox"
    event_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload_json: Mapped[str] = mapped_column(Text)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(24), index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at_ms: Mapped[int] = mapped_column(BigInteger)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger)
    response_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)


class OutcomeRow(Base):
    __tablename__ = "outcomes"
    __table_args__ = (UniqueConstraint("event_id", "horizon_seconds", name="uq_outcome_horizon"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(32), index=True)
    horizon_seconds: Mapped[int] = mapped_column(Integer)
    mfe: Mapped[float] = mapped_column(Float)
    mae: Mapped[float] = mapped_column(Float)
    close_return: Mapped[float] = mapped_column(Float)
    observed_until_ms: Mapped[int] = mapped_column(BigInteger)


class ShadowObservationRow(Base):
    """One durable prospective comparator observation for a raw C0 opportunity.

    Idempotent by ``observation_id``. Replaying the same opportunity with the
    same canonical payload is a no-op; reusing the ID with different content is
    a hard conflict so evidence can never be silently overwritten.
    """

    __tablename__ = "shadow_observations"
    observation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    campaign_id: Mapped[str] = mapped_column(String(64), index=True)
    opportunity_id: Mapped[str] = mapped_column(String(64), index=True)
    market: Mapped[str] = mapped_column(String(16), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    family: Mapped[str] = mapped_column(String(32))
    direction: Mapped[str] = mapped_column(String(16))
    decision_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    primary_interval: Mapped[str] = mapped_column(String(8))
    payload_json: Mapped[str] = mapped_column(Text)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    policy_sha256: Mapped[str] = mapped_column(String(64))
    created_at_ms: Mapped[int] = mapped_column(BigInteger)


class ShadowCoverageRow(Base):
    """Compact per-close coverage ledger proving no silent observation holes."""

    __tablename__ = "shadow_coverage"
    __table_args__ = (
        UniqueConstraint(
            "campaign_id",
            "market",
            "decision_close_ms",
            "primary_interval",
            name="uq_shadow_coverage_cell",
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[str] = mapped_column(String(64), index=True)
    market: Mapped[str] = mapped_column(String(16), index=True)
    decision_close_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    primary_interval: Mapped[str] = mapped_column(String(8))
    expected_tradable_count: Mapped[int] = mapped_column(Integer)
    tradable_universe_hash: Mapped[str] = mapped_column(String(64))
    mature_count: Mapped[int] = mapped_column(Integer)
    htf_ready_count: Mapped[int] = mapped_column(Integer)
    fresh_bbo_count: Mapped[int] = mapped_column(Integer)
    raw_c0_count: Mapped[int] = mapped_column(Integer)
    comparator_rows: Mapped[int] = mapped_column(Integer)
    complete: Mapped[bool] = mapped_column(Boolean)
    failures_json: Mapped[str] = mapped_column(Text)
    content_sha256: Mapped[str] = mapped_column(String(64))
