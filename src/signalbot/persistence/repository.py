from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import create_engine, desc, func, inspect, select, update
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
    ShadowCampaignRow,
    ShadowCoverageRow,
    ShadowObservationRow,
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


def _json_loads(text: str) -> list[str]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []


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


def _canonical_campaign_manifest(
    *,
    campaign_schema_version: str,
    campaign_id: str,
    campaign_mode: str,
    source_identity: str,
    rule_version: str,
    policy_name: str,
    policy_version: str,
    policy_sha256: str,
    config_sha256: str,
    observation_schema_version: str,
    primary_interval: str,
    markets: list[str],
    families: dict[str, list[str]],
    activation_ms: int | None,
    created_at_ms: int,
) -> tuple[str, str]:
    """Build the canonical immutable campaign manifest and its SHA-256.

    The manifest binds every scientific/provenance field that determines the
    evidence population. Registration is byte-committed on this canonical JSON,
    so any later change to a campaign's scientific content must use a NEW
    campaign_id rather than mutating the manifest.
    """

    canonical = {
        "campaign_schema_version": campaign_schema_version,
        "campaign_id": campaign_id,
        "campaign_mode": campaign_mode,
        "source_identity": source_identity,
        "rule_version": rule_version,
        "policy_name": policy_name,
        "policy_version": policy_version,
        "policy_sha256": policy_sha256,
        "config_sha256": config_sha256,
        "observation_schema_version": observation_schema_version,
        "primary_interval": primary_interval,
        "markets": sorted(markets),
        "families": {
            market: sorted(values)
            for market, values in sorted(families.items())
        },
        "activation_ms": activation_ms,
        "created_at_ms": created_at_ms,
    }
    manifest_json = json.dumps(
        canonical, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    )
    manifest_sha256 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
    return manifest_json, manifest_sha256


def _coverage_content_sha256(
    *,
    campaign_id: str,
    market: str,
    decision_close_ms: int,
    primary_interval: str,
    expected_tradable_count: int,
    tradable_universe_hash: str,
    mature_count: int,
    htf_ready_count: int,
    fresh_bbo_count: int,
    raw_c0_count: int,
    comparator_rows: int,
    evidence_failures: int,
    seen_symbols: list[str],
    complete: bool,
    failures: list[str],
) -> str:
    """Canonical SHA-256 over the immutable scientific content of a SEALED cell."""

    canonical = {
        "campaign_id": campaign_id,
        "market": market,
        "decision_close_ms": decision_close_ms,
        "primary_interval": primary_interval,
        "expected_tradable_count": expected_tradable_count,
        "tradable_universe_hash": tradable_universe_hash,
        "mature_count": mature_count,
        "htf_ready_count": htf_ready_count,
        "fresh_bbo_count": fresh_bbo_count,
        "raw_c0_count": raw_c0_count,
        "comparator_rows": comparator_rows,
        "evidence_failures": evidence_failures,
        "seen_symbols": sorted(seen_symbols),
        "complete": complete,
        "failures": sorted(failures),
    }
    payload_json = json.dumps(
        canonical, separators=(",", ":"), sort_keys=True, ensure_ascii=False
    )
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _migrate_sqlite_shadow_schema(engine: Engine) -> None:
    """Add supported shadow-evidence columns without deleting existing rows."""

    if engine.dialect.name != "sqlite":
        return
    additions: dict[str, dict[str, str]] = {
        "shadow_observations": {
            "campaign_manifest_sha256": "VARCHAR(64) NOT NULL DEFAULT ''",
        },
        "shadow_coverage": {
            "status": "VARCHAR(16) NOT NULL DEFAULT 'OPEN'",
            "seen_symbols_json": "TEXT NOT NULL DEFAULT '[]'",
            "first_seen_ms": "BIGINT NOT NULL DEFAULT 0",
            "sealed_at_ms": "BIGINT",
            "content_sha256": "VARCHAR(64) NOT NULL DEFAULT ''",
            "evidence_failures": "INTEGER NOT NULL DEFAULT 0",
            "campaign_manifest_sha256": "VARCHAR(64) NOT NULL DEFAULT ''",
        },
        "shadow_campaigns": {
            "campaign_schema_version": "VARCHAR(16) NOT NULL DEFAULT ''",
            "policy_name": "VARCHAR(64) NOT NULL DEFAULT ''",
            "primary_interval": "VARCHAR(8) NOT NULL DEFAULT ''",
            "markets_json": "TEXT NOT NULL DEFAULT '[]'",
            "families_json": "TEXT NOT NULL DEFAULT '{}'",
            "manifest_json": "TEXT NOT NULL DEFAULT ''",
            "manifest_sha256": "VARCHAR(64) NOT NULL DEFAULT ''",
        },
    }
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        for table_name, columns in additions.items():
            if table_name not in tables:
                continue
            existing = {
                column["name"] for column in inspect(engine).get_columns(table_name)
            }
            for column_name, column_ddl in columns.items():
                if column_name in existing:
                    continue
                connection.exec_driver_sql(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_ddl}'
                )


def _require_shadow_campaign(
    session: Session,
    *,
    campaign_id: str,
    campaign_manifest_sha256: str,
) -> ShadowCampaignRow:
    """Require one already-registered immutable campaign for evidence writes."""

    row = session.get(ShadowCampaignRow, campaign_id)
    if row is None:
        raise EventIdConflictError(
            f"shadow evidence references unregistered campaign {campaign_id}"
        )
    if not campaign_manifest_sha256 or row.manifest_sha256 != campaign_manifest_sha256:
        raise EventIdConflictError(
            f"shadow evidence campaign manifest mismatch for {campaign_id}"
        )
    return row


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
        _migrate_sqlite_shadow_schema(self._engine)
        self.ready = True

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
        self.ready = False

    def register_shadow_campaign(
        self,
        *,
        campaign_schema_version: str,
        campaign_id: str,
        campaign_mode: str,
        source_identity: str,
        rule_version: str,
        policy_name: str,
        policy_version: str,
        policy_sha256: str,
        config_sha256: str,
        observation_schema_version: str,
        primary_interval: str,
        markets: list[str],
        families: dict[str, list[str]],
        activation_ms: int | None,
        created_at_ms: int,
        status: str,
    ) -> bool:
        """Persist one append-once immutable campaign manifest.

        Returns True when a new campaign was created. A byte-identical
        re-registration is an idempotent no-op. Reusing an existing campaign_id
        with any different scientific or provenance content raises
        EventIdConflictError. There is intentionally no ordinary API to update
        a campaign scientific parameters once registered.
        """

        mode = campaign_mode.lower()
        manifest_json, manifest_sha256 = _canonical_campaign_manifest(
            campaign_schema_version=campaign_schema_version,
            campaign_id=campaign_id,
            campaign_mode=mode,
            source_identity=source_identity,
            rule_version=rule_version,
            policy_name=policy_name,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
            config_sha256=config_sha256,
            observation_schema_version=observation_schema_version,
            primary_interval=primary_interval,
            markets=markets,
            families=families,
            activation_ms=activation_ms,
            created_at_ms=created_at_ms,
        )
        with Session(self.engine) as session:
            existing = session.get(ShadowCampaignRow, campaign_id)
            if existing is not None:
                if existing.manifest_sha256 != manifest_sha256:
                    raise EventIdConflictError(
                        "shadow campaign " + campaign_id + " maps to a different manifest"
                    )
                return False
            session.add(
                ShadowCampaignRow(
                    campaign_schema_version=campaign_schema_version,
                    campaign_id=campaign_id,
                    campaign_mode=mode,
                    source_identity=source_identity,
                    rule_version=rule_version,
                    policy_name=policy_name,
                    policy_sha256=policy_sha256,
                    config_sha256=config_sha256,
                    observation_schema_version=observation_schema_version,
                    policy_version=policy_version,
                    primary_interval=primary_interval,
                    markets_json=json.dumps(
                        sorted(markets),
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                    families_json=json.dumps(
                        {
                            market: sorted(values)
                            for market, values in sorted(families.items())
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                        ensure_ascii=False,
                    ),
                    activation_ms=activation_ms,
                    created_at_ms=created_at_ms,
                    status=status,
                    manifest_json=manifest_json,
                    manifest_sha256=manifest_sha256,
                )
            )
            session.commit()
            return True

    def get_shadow_campaign(self, campaign_id: str) -> dict[str, Any] | None:
        """Return the registered campaign manifest, or None when absent."""

        with Session(self.engine) as session:
            row = session.get(ShadowCampaignRow, campaign_id)
            if row is None:
                return None
            return {
                "campaign_id": row.campaign_id,
                "campaign_schema_version": row.campaign_schema_version,
                "campaign_mode": row.campaign_mode,
                "source_identity": row.source_identity,
                "rule_version": row.rule_version,
                "policy_name": row.policy_name,
                "policy_version": row.policy_version,
                "policy_sha256": row.policy_sha256,
                "config_sha256": row.config_sha256,
                "observation_schema_version": row.observation_schema_version,
                "primary_interval": row.primary_interval,
                "markets": _json_loads(row.markets_json),
                "families": json.loads(row.families_json),
                "activation_ms": row.activation_ms,
                "created_at_ms": row.created_at_ms,
                "status": row.status,
                "manifest_sha256": row.manifest_sha256,
            }

    def assert_shadow_campaign_matches(
        self,
        *,
        campaign_id: str,
        campaign_schema_version: str,
        campaign_mode: str,
        source_identity: str,
        rule_version: str,
        policy_name: str,
        policy_version: str,
        policy_sha256: str,
        config_sha256: str,
        observation_schema_version: str,
        primary_interval: str,
        markets: list[str],
        families: dict[str, list[str]],
        activation_ms: int | None,
        created_at_ms: int,
    ) -> None:
        """Verify a registered campaign matches the requested manifest exactly.

        Raises EventIdConflictError on any provenance or scientific mismatch so
        prospective collection can fail closed before admitting observations.
        A missing campaign also raises EventIdConflictError.
        """

        _, expected_sha256 = _canonical_campaign_manifest(
            campaign_schema_version=campaign_schema_version,
            campaign_id=campaign_id,
            campaign_mode=campaign_mode.lower(),
            source_identity=source_identity,
            rule_version=rule_version,
            policy_name=policy_name,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
            config_sha256=config_sha256,
            observation_schema_version=observation_schema_version,
            primary_interval=primary_interval,
            markets=markets,
            families=families,
            activation_ms=activation_ms,
            created_at_ms=created_at_ms,
        )
        with Session(self.engine) as session:
            row = session.get(ShadowCampaignRow, campaign_id)
            if row is None:
                raise EventIdConflictError(
                    "shadow campaign " + campaign_id + " is not registered"
                )
            if row.manifest_sha256 != expected_sha256:
                raise EventIdConflictError(
                    "shadow campaign "
                    + campaign_id
                    + " does not match requested manifest"
                )

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

    def save_shadow_observation(
        self,
        *,
        observation_id: str,
        campaign_id: str,
        opportunity_id: str,
        market: str,
        symbol: str,
        family: str,
        direction: str,
        decision_time_ms: int,
        primary_interval: str,
        payload: dict[str, Any],
        policy_sha256: str,
        campaign_manifest_sha256: str,
        created_at_ms: int,
    ) -> bool:
        """Persist one prospective shadow comparator observation.

        Returns ``True`` when a new row was created. Replaying the same
        ``observation_id`` with byte-identical canonical content is a no-op.
        Reusing the ID for different content raises ``EventIdConflictError``.
        """

        payload_text = json.dumps(
            payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False
        )
        fingerprint = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            _require_shadow_campaign(
                session,
                campaign_id=campaign_id,
                campaign_manifest_sha256=campaign_manifest_sha256,
            )
            existing = session.get(ShadowObservationRow, observation_id)
            if existing is not None:
                if (
                    existing.payload_sha256 != fingerprint
                    or existing.payload_json != payload_text
                    or existing.policy_sha256 != policy_sha256
                    or existing.campaign_manifest_sha256 != campaign_manifest_sha256
                ):
                    raise EventIdConflictError(
                        f"shadow observation {observation_id} maps to conflicting evidence"
                    )
                return False
            session.add(
                ShadowObservationRow(
                    observation_id=observation_id,
                    campaign_id=campaign_id,
                    opportunity_id=opportunity_id,
                    market=market,
                    symbol=symbol,
                    family=family,
                    direction=direction,
                    decision_time_ms=decision_time_ms,
                    primary_interval=primary_interval,
                    payload_json=payload_text,
                    payload_sha256=fingerprint,
                    policy_sha256=policy_sha256,
                    campaign_manifest_sha256=campaign_manifest_sha256,
                    created_at_ms=created_at_ms,
                )
            )
            session.commit()
            return True

    def count_shadow_observations(
        self, *, campaign_id: str, decision_time_ms: int | None = None
    ) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(ShadowObservationRow).where(
                ShadowObservationRow.campaign_id == campaign_id
            )
            if decision_time_ms is not None:
                statement = statement.where(
                    ShadowObservationRow.decision_time_ms == decision_time_ms
                )
            return int(session.scalar(statement) or 0)

    def get_shadow_coverage(
        self,
        *,
        campaign_id: str,
        market: str,
        decision_close_ms: int,
        primary_interval: str,
    ) -> dict[str, Any] | None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(ShadowCoverageRow).where(
                    ShadowCoverageRow.campaign_id == campaign_id,
                    ShadowCoverageRow.market == market,
                    ShadowCoverageRow.decision_close_ms == decision_close_ms,
                    ShadowCoverageRow.primary_interval == primary_interval,
                )
            )
            if row is None:
                return None
            return {
                "mature_count": row.mature_count,
                "expected_tradable_count": row.expected_tradable_count,
                "htf_ready_count": row.htf_ready_count,
                "fresh_bbo_count": row.fresh_bbo_count,
                "raw_c0_count": row.raw_c0_count,
                "comparator_rows": row.comparator_rows,
                "evidence_failures": row.evidence_failures,
                "seen_symbols": _json_loads(row.seen_symbols_json),
                "campaign_manifest_sha256": row.campaign_manifest_sha256,
                "content_sha256": row.content_sha256,
                "first_seen_ms": row.first_seen_ms,
                "sealed_at_ms": row.sealed_at_ms,
                "status": row.status,
                "complete": row.complete,
                "failures": row.failures_json,
            }

    def begin_shadow_coverage(
        self,
        *,
        campaign_id: str,
        market: str,
        decision_close_ms: int,
        primary_interval: str,
        expected_tradable_count: int,
        tradable_universe_hash: str,
        campaign_manifest_sha256: str,
        first_seen_ms: int,
    ) -> None:
        """Insert a durable OPEN provisional coverage cell (idempotent).

        The provisional cell proves that a close began being observed even if
        the process crashes before it is sealed, so a restart can discover it
        and mark it INCOMPLETE instead of silently losing the coverage window.

        An existing cell with identical immutable metadata is an idempotent
        no-op. The same cell key observed under different immutable metadata
        (universe, expected tradable count, interval) raises
        EventIdConflictError so universe drift is never silently accepted.
        """

        with Session(self.engine) as session:
            _require_shadow_campaign(
                session,
                campaign_id=campaign_id,
                campaign_manifest_sha256=campaign_manifest_sha256,
            )
            existing = session.scalar(
                select(ShadowCoverageRow).where(
                    ShadowCoverageRow.campaign_id == campaign_id,
                    ShadowCoverageRow.market == market,
                    ShadowCoverageRow.decision_close_ms == decision_close_ms,
                    ShadowCoverageRow.primary_interval == primary_interval,
                )
            )
            if existing is not None:
                if (
                    existing.expected_tradable_count != expected_tradable_count
                    or existing.tradable_universe_hash != tradable_universe_hash
                    or existing.primary_interval != primary_interval
                    or existing.campaign_manifest_sha256 != campaign_manifest_sha256
                ):
                    raise EventIdConflictError(
                        "shadow coverage cell immutable metadata conflict for "
                        + campaign_id
                        + "/"
                        + market
                        + "/"
                        + str(decision_close_ms)
                    )
                return
            session.add(
                ShadowCoverageRow(
                    campaign_id=campaign_id,
                    campaign_manifest_sha256=campaign_manifest_sha256,
                    market=market,
                    decision_close_ms=decision_close_ms,
                    primary_interval=primary_interval,
                    expected_tradable_count=expected_tradable_count,
                    tradable_universe_hash=tradable_universe_hash,
                    mature_count=0,
                    htf_ready_count=0,
                    fresh_bbo_count=0,
                    raw_c0_count=0,
                    comparator_rows=0,
                    evidence_failures=0,
                    complete=False,
                    failures_json="[]",
                    content_sha256="",
                    status="OPEN",
                    seen_symbols_json="[]",
                    first_seen_ms=first_seen_ms,
                    sealed_at_ms=None,
                )
            )
            session.commit()

    def save_shadow_coverage(
        self,
        *,
        campaign_id: str,
        market: str,
        decision_close_ms: int,
        primary_interval: str,
        expected_tradable_count: int,
        tradable_universe_hash: str,
        campaign_manifest_sha256: str,
        mature_count: int,
        htf_ready_count: int,
        fresh_bbo_count: int,
        raw_c0_count: int,
        comparator_rows: int,
        evidence_failures: int = 0,
        seen_symbols: list[str] | None = None,
        sealed_at_ms: int | None = None,
        complete: bool,
        failures: list[str],
        created_at_ms: int,
    ) -> None:
        """Seal one coverage cell to SEALED with its final durable counters.

        begin_shadow_coverage writes the provisional OPEN cell; this method
        transitions that cell to SEALED and records the final counters, unique
        seen symbols, completeness and a canonical content_sha256. It verifies
        that the immutable OPEN metadata (expected count, universe hash,
        campaign/market/interval/close identity) matches the call arguments,
        and is conflict-loud on any drift from an already-sealed cell.
        """

        seen = sorted(set(seen_symbols or []))
        content_sha256 = _coverage_content_sha256(
            campaign_id=campaign_id,
            market=market,
            decision_close_ms=decision_close_ms,
            primary_interval=primary_interval,
            expected_tradable_count=expected_tradable_count,
            tradable_universe_hash=tradable_universe_hash,
            mature_count=mature_count,
            htf_ready_count=htf_ready_count,
            fresh_bbo_count=fresh_bbo_count,
            raw_c0_count=raw_c0_count,
            comparator_rows=comparator_rows,
            evidence_failures=evidence_failures,
            seen_symbols=seen,
            complete=complete,
            failures=failures,
        )
        with Session(self.engine) as session:
            _require_shadow_campaign(
                session,
                campaign_id=campaign_id,
                campaign_manifest_sha256=campaign_manifest_sha256,
            )
            existing = session.scalar(
                select(ShadowCoverageRow).where(
                    ShadowCoverageRow.campaign_id == campaign_id,
                    ShadowCoverageRow.market == market,
                    ShadowCoverageRow.decision_close_ms == decision_close_ms,
                    ShadowCoverageRow.primary_interval == primary_interval,
                )
            )
            if existing is not None:
                if existing.status == "SEALED":
                    if existing.content_sha256 != content_sha256:
                        raise EventIdConflictError(
                            "shadow coverage cell already sealed with different content for "
                            + campaign_id
                            + "/"
                            + market
                            + "/"
                            + str(decision_close_ms)
                        )
                    return
                if (
                    existing.expected_tradable_count != expected_tradable_count
                    or existing.tradable_universe_hash != tradable_universe_hash
                    or existing.primary_interval != primary_interval
                    or existing.campaign_manifest_sha256 != campaign_manifest_sha256
                ):
                    raise EventIdConflictError(
                        "shadow coverage cell immutable metadata mismatch on seal for "
                        + campaign_id
                        + "/"
                        + market
                        + "/"
                        + str(decision_close_ms)
                    )
                existing.mature_count = mature_count
                existing.htf_ready_count = htf_ready_count
                existing.fresh_bbo_count = fresh_bbo_count
                existing.raw_c0_count = raw_c0_count
                existing.comparator_rows = comparator_rows
                existing.evidence_failures = evidence_failures
                existing.seen_symbols_json = json.dumps(
                    seen, separators=(",", ":"), ensure_ascii=False
                )
                existing.complete = complete
                existing.failures_json = json.dumps(
                    failures, separators=(",", ":"), ensure_ascii=False
                )
                existing.content_sha256 = content_sha256
                existing.status = "SEALED"
                existing.sealed_at_ms = sealed_at_ms
                session.commit()
                return
            raise EventIdConflictError(
                "shadow coverage cell must be begun (OPEN) before sealing "
                + campaign_id
                + "/"
                + market
                + "/"
                + str(decision_close_ms)
            )

    def update_shadow_coverage_progress(
        self,
        *,
        campaign_id: str,
        market: str,
        decision_close_ms: int,
        primary_interval: str,
        campaign_manifest_sha256: str,
        mature_count: int,
        htf_ready_count: int,
        fresh_bbo_count: int,
        raw_c0_count: int,
        comparator_rows: int,
        evidence_failures: int,
        seen_symbols: list[str],
    ) -> None:
        """Durably persist in-progress coverage counters for an OPEN cell.

        Called as each unique symbol is observed, so an abrupt crash leaves the
        OPEN provisional row carrying an auditable snapshot of how far the cell
        progressed (which symbols were seen, how many were mature, etc.) rather
        than only proving that the cell was interrupted.

        Integrity is loud, not silent: a missing cell, a SEALED cell receiving
        new progress, monotonic counter regression, or a mathematically
        impossible counter shape each raise EventIdConflictError so scientific
        history is never silently rewritten backwards.
        """

        seen = sorted(set(seen_symbols))
        if min(
            mature_count,
            htf_ready_count,
            fresh_bbo_count,
            raw_c0_count,
            comparator_rows,
            evidence_failures,
        ) < 0:
            raise EventIdConflictError("shadow coverage counters cannot be negative")
        if mature_count != len(seen):
            raise EventIdConflictError(
                "shadow coverage mature_count must equal unique seen-symbol count"
            )
        if htf_ready_count > mature_count or fresh_bbo_count > mature_count:
            raise EventIdConflictError(
                "shadow coverage readiness counters cannot exceed mature_count"
            )
        if comparator_rows > raw_c0_count:
            raise EventIdConflictError(
                "shadow coverage comparator_rows cannot exceed raw_c0_count for "
                + campaign_id
                + "/"
                + market
            )
        if raw_c0_count > mature_count:
            raise EventIdConflictError(
                "shadow coverage raw_c0_count cannot exceed mature_count for "
                + campaign_id
                + "/"
                + market
            )
        with Session(self.engine) as session:
            _require_shadow_campaign(
                session,
                campaign_id=campaign_id,
                campaign_manifest_sha256=campaign_manifest_sha256,
            )
            existing = session.scalar(
                select(ShadowCoverageRow).where(
                    ShadowCoverageRow.campaign_id == campaign_id,
                    ShadowCoverageRow.market == market,
                    ShadowCoverageRow.decision_close_ms == decision_close_ms,
                    ShadowCoverageRow.primary_interval == primary_interval,
                )
            )
            if existing is None:
                raise EventIdConflictError(
                    "shadow coverage cell missing during progress update for "
                    + campaign_id
                    + "/"
                    + market
                    + "/"
                    + str(decision_close_ms)
                )
            if existing.status != "OPEN":
                raise EventIdConflictError(
                    "shadow coverage progress attempted on non-OPEN cell for "
                    + campaign_id
                    + "/"
                    + market
                    + "/"
                    + str(decision_close_ms)
                    + " status="
                    + existing.status
                )
            prev_mature = existing.mature_count
            prev_htf_ready = existing.htf_ready_count
            prev_fresh_bbo = existing.fresh_bbo_count
            prev_raw = existing.raw_c0_count
            prev_comparator = existing.comparator_rows
            prev_evidence_failures = existing.evidence_failures
            if (
                mature_count < prev_mature
                or htf_ready_count < prev_htf_ready
                or fresh_bbo_count < prev_fresh_bbo
                or raw_c0_count < prev_raw
                or comparator_rows < prev_comparator
                or evidence_failures < prev_evidence_failures
            ):
                raise EventIdConflictError(
                    "shadow coverage counter regression for "
                    + campaign_id
                    + "/"
                    + market
                    + "/"
                    + str(decision_close_ms)
                )
            prev_seen = set(_json_loads(existing.seen_symbols_json))
            if not prev_seen.issubset(seen):
                raise EventIdConflictError(
                    "shadow coverage seen-symbol set regression for "
                    + campaign_id
                    + "/"
                    + market
                    + "/"
                    + str(decision_close_ms)
                )
            existing.mature_count = mature_count
            existing.htf_ready_count = htf_ready_count
            existing.fresh_bbo_count = fresh_bbo_count
            existing.raw_c0_count = raw_c0_count
            existing.comparator_rows = comparator_rows
            existing.evidence_failures = evidence_failures
            existing.seen_symbols_json = json.dumps(
                seen, separators=(",", ":"), ensure_ascii=False
            )
            session.commit()

    def seal_stale_open_cells(
        self,
        campaign_id: str,
        *,
        campaign_manifest_sha256: str,
        sealed_at_ms: int,
    ) -> int:
        """Seal any leftover OPEN cells from a prior process as INCOMPLETE.

        Called at observer construction so an abrupt restart never silently
        leaves an unsealed cell (which the old code would simply lose). Every
        prior provisional cell is marked SEALED and incomplete to prove the
        restart audited and closed it.
        """

        sealed = 0
        with Session(self.engine) as session:
            _require_shadow_campaign(
                session,
                campaign_id=campaign_id,
                campaign_manifest_sha256=campaign_manifest_sha256,
            )
            rows = (
                session.execute(
                    select(ShadowCoverageRow).where(
                        ShadowCoverageRow.campaign_id == campaign_id,
                        ShadowCoverageRow.status == "OPEN",
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                failures = _json_loads(row.failures_json)
                restart_failure = "interrupted restart: coverage cell left open, marked incomplete"
                if restart_failure not in failures:
                    failures.append(restart_failure)
                if row.campaign_manifest_sha256 != campaign_manifest_sha256:
                    failures.append(
                        "campaign manifest unavailable or mismatched after schema migration"
                    )
                row.status = "SEALED"
                row.complete = False
                row.failures_json = json.dumps(
                    failures,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                row.content_sha256 = _coverage_content_sha256(
                    campaign_id=row.campaign_id,
                    market=row.market,
                    decision_close_ms=row.decision_close_ms,
                    primary_interval=row.primary_interval,
                    expected_tradable_count=row.expected_tradable_count,
                    tradable_universe_hash=row.tradable_universe_hash,
                    mature_count=row.mature_count,
                    htf_ready_count=row.htf_ready_count,
                    fresh_bbo_count=row.fresh_bbo_count,
                    raw_c0_count=row.raw_c0_count,
                    comparator_rows=row.comparator_rows,
                    evidence_failures=row.evidence_failures,
                    seen_symbols=_json_loads(row.seen_symbols_json),
                    complete=False,
                    failures=failures,
                )
                row.sealed_at_ms = sealed_at_ms
                sealed += 1
            session.commit()
        return sealed
