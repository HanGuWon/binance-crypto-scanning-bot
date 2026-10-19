from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import create_engine, desc, func, select, update
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.orm import Session, aliased
from sqlalchemy.pool import StaticPool

from signalbot.domain.enums import Market, SignalFamily
from signalbot.domain.models import (
    Candle,
    GainerWatchState,
    Instrument,
    RuleEvaluation,
    SignalDecision,
)
from signalbot.persistence.models import (
    AlertOutboxRow,
    AlertRow,
    Base,
    CandleRow,
    GainerWatchRow,
    MarketWatchEpisodeRow,
    OutcomeRow,
    PublicStructureFlowCampaignRow,
    PublicStructureFlowOpportunityRow,
    PublicStructureFlowShadowRow,
    SignalRow,
)


class EventIdConflictError(RuntimeError):
    """Raised when one deterministic event ID maps to different content."""


class OutboxCapacityError(RuntimeError):
    """Raised before persisting a signal when active delivery intent is full."""


class PublicStructureFlowConflictError(RuntimeError):
    """Raised when one research observation ID maps to different evidence."""


class PublicStructureFlowCampaignConflictError(RuntimeError):
    """Raised when a frozen campaign ID maps to a different manifest."""


@dataclass(frozen=True, slots=True)
class PublicStructureFlowTerminalWrite:
    opportunity_id: str
    observation_id: str
    campaign_id: str
    market: Market
    symbol: str
    timeframe: str
    event_time_ms: int
    candidate_version: str
    payload: dict[str, object]
    status: str
    reason: str | None
    ready_at_ms: int
    recorded_at_ms: int


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
    return json.dumps(decision.model_dump(mode="json"), separators=(",", ":"), ensure_ascii=False)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _stable_shadow_payload(value: dict[str, object]) -> dict[str, object]:
    """Strip legacy process-timing fields that are not part of the research result."""

    return {
        key: item
        for key, item in value.items()
        if key not in {"ready_at_ms", "terminal_reason"}
    }


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

    def register_public_structure_flow_campaign(
        self,
        *,
        campaign_id: str,
        candidate_version: str,
        activation_ms: int,
        contract_sha256: str,
        manifest: dict[str, object],
        registered_at_ms: int,
    ) -> bool:
        """Register one immutable prospective campaign manifest."""

        manifest_json = json.dumps(
            manifest,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        manifest_sha256 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            existing = session.get(PublicStructureFlowCampaignRow, campaign_id)
            if existing is not None:
                if (
                    existing.candidate_version != candidate_version
                    or existing.activation_ms != activation_ms
                    or existing.contract_sha256 != contract_sha256
                    or existing.manifest_sha256 != manifest_sha256
                    or existing.manifest_json != manifest_json
                ):
                    raise PublicStructureFlowCampaignConflictError(
                        f"campaign ID {campaign_id} maps to a conflicting manifest"
                    )
                return False
            if activation_ms < registered_at_ms:
                raise ValueError(
                    "new public structure-flow campaign activation must not precede registration"
                )
            session.add(
                PublicStructureFlowCampaignRow(
                    campaign_id=campaign_id,
                    candidate_version=candidate_version,
                    activation_ms=activation_ms,
                    contract_sha256=contract_sha256,
                    manifest_json=manifest_json,
                    manifest_sha256=manifest_sha256,
                    registered_at_ms=registered_at_ms,
                )
            )
            session.commit()
            return True

    def save_public_structure_flow_shadow(
        self,
        *,
        observation_id: str,
        campaign_id: str,
        market: Market,
        symbol: str,
        timeframe: str,
        event_time_ms: int,
        candidate_version: str,
        payload: dict[str, object],
        recorded_at_ms: int,
    ) -> bool:
        """Append one immutable research-only observation; never creates an outbox row."""

        payload_json = _canonical_json(payload)
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            campaign = session.get(PublicStructureFlowCampaignRow, campaign_id)
            if campaign is None:
                raise PublicStructureFlowConflictError(
                    f"unregistered public structure-flow campaign: {campaign_id}"
                )
            if candidate_version != campaign.candidate_version:
                raise PublicStructureFlowConflictError(
                    "shadow candidate version does not match registered campaign"
                )
            if event_time_ms < campaign.activation_ms:
                raise PublicStructureFlowConflictError(
                    "shadow event precedes registered campaign activation"
                )
            if recorded_at_ms < event_time_ms:
                raise PublicStructureFlowConflictError(
                    "shadow recorded time precedes event time"
                )
            manifest_value = json.loads(campaign.manifest_json)
            if not isinstance(manifest_value, dict):  # pragma: no cover
                raise PublicStructureFlowConflictError(
                    "registered campaign manifest is not an object"
                )
            required_payload = {
                "campaign_id": campaign_id,
                "candidate_version": candidate_version,
                "market": market.value,
                "symbol": symbol.upper(),
                "timeframe": timeframe,
                "event_time_ms": event_time_ms,
                "activation_ms": campaign.activation_ms,
                "contract_sha256": campaign.contract_sha256,
                "manifest_sha256": campaign.manifest_sha256,
                "production_entry": False,
                "discord_delivery": False,
                "order_placement": False,
            }
            for key, expected in required_payload.items():
                if payload.get(key) != expected:
                    raise PublicStructureFlowConflictError(
                        f"shadow payload field {key} does not match repository authority"
                    )
            for key in ("config_sha256", "rule_version", "source_identity"):
                if payload.get(key) != manifest_value.get(key):
                    raise PublicStructureFlowConflictError(
                        f"shadow payload field {key} does not match campaign manifest"
                    )
            if payload.get("schema_version") != manifest_value.get("schema_version"):
                raise PublicStructureFlowConflictError(
                    "shadow payload schema_version does not match campaign manifest"
                )
            expected_observation_id = hashlib.sha256(
                "\0".join(
                    (
                        campaign_id,
                        candidate_version,
                        market.value,
                        symbol.upper(),
                        timeframe,
                        str(event_time_ms),
                    )
                ).encode("utf-8")
            ).hexdigest()[:32]
            if observation_id != expected_observation_id:
                raise PublicStructureFlowConflictError(
                    "shadow observation ID does not match deterministic identity"
                )
            existing = session.get(PublicStructureFlowShadowRow, observation_id)
            if existing is not None:
                existing_value = json.loads(existing.payload_json)
                if not isinstance(existing_value, dict):  # pragma: no cover
                    raise PublicStructureFlowConflictError(
                        "existing shadow payload is not an object"
                    )
                stable_existing = _canonical_json(
                    _stable_shadow_payload(cast(dict[str, object], existing_value))
                )
                stable_incoming = _canonical_json(_stable_shadow_payload(payload))
                if (
                    existing.campaign_id != campaign_id
                    or existing.market != market.value
                    or existing.symbol != symbol.upper()
                    or existing.timeframe != timeframe
                    or existing.event_time_ms != event_time_ms
                    or existing.candidate_version != candidate_version
                    or stable_existing != stable_incoming
                ):
                    raise PublicStructureFlowConflictError(
                        f"observation ID {observation_id} maps to conflicting research payloads"
                    )
                return False
            session.add(
                PublicStructureFlowShadowRow(
                    observation_id=observation_id,
                    campaign_id=campaign_id,
                    market=market.value,
                    symbol=symbol.upper(),
                    timeframe=timeframe,
                    event_time_ms=event_time_ms,
                    candidate_version=candidate_version,
                    payload_json=payload_json,
                    payload_sha256=payload_sha256,
                    recorded_at_ms=recorded_at_ms,
                )
            )
            session.commit()
            return True

    def _apply_public_structure_flow_terminal_write(
        self,
        session: Session,
        write: PublicStructureFlowTerminalWrite,
    ) -> bool:
        """Apply observation append + denominator terminalization in one transaction."""

        if write.status not in {"RECORDED", "RECORDED_DEGRADED"}:
            raise ValueError(f"unsupported recorded terminal status: {write.status}")
        if write.ready_at_ms < write.event_time_ms:
            raise PublicStructureFlowConflictError("ready time precedes opportunity event")
        if write.recorded_at_ms < write.event_time_ms:
            raise PublicStructureFlowConflictError("shadow recorded time precedes event time")

        campaign = session.get(PublicStructureFlowCampaignRow, write.campaign_id)
        if campaign is None:
            raise PublicStructureFlowConflictError(
                f"unregistered public structure-flow campaign: {write.campaign_id}"
            )
        manifest_value = json.loads(campaign.manifest_json)
        if not isinstance(manifest_value, dict):  # pragma: no cover
            raise PublicStructureFlowConflictError("registered campaign manifest is not an object")
        if write.candidate_version != campaign.candidate_version:
            raise PublicStructureFlowConflictError(
                "shadow candidate version does not match registered campaign"
            )
        if write.event_time_ms < campaign.activation_ms:
            raise PublicStructureFlowConflictError(
                "shadow event precedes registered campaign activation"
            )
        required_payload = {
            "campaign_id": write.campaign_id,
            "candidate_version": write.candidate_version,
            "market": write.market.value,
            "symbol": write.symbol.upper(),
            "timeframe": write.timeframe,
            "event_time_ms": write.event_time_ms,
            "activation_ms": campaign.activation_ms,
            "contract_sha256": campaign.contract_sha256,
            "manifest_sha256": campaign.manifest_sha256,
            "production_entry": False,
            "discord_delivery": False,
            "order_placement": False,
        }
        for key, expected in required_payload.items():
            if write.payload.get(key) != expected:
                raise PublicStructureFlowConflictError(
                    f"shadow payload field {key} does not match repository authority"
                )
        for key in ("config_sha256", "rule_version", "source_identity"):
            if write.payload.get(key) != manifest_value.get(key):
                raise PublicStructureFlowConflictError(
                    f"shadow payload field {key} does not match campaign manifest"
                )
        if write.payload.get("schema_version") != manifest_value.get("schema_version"):
            raise PublicStructureFlowConflictError(
                "shadow payload schema_version does not match campaign manifest"
            )
        expected_observation_id = hashlib.sha256(
            "\0".join(
                (
                    write.campaign_id,
                    write.candidate_version,
                    write.market.value,
                    write.symbol.upper(),
                    write.timeframe,
                    str(write.event_time_ms),
                )
            ).encode("utf-8")
        ).hexdigest()[:32]
        if write.observation_id != expected_observation_id:
            raise PublicStructureFlowConflictError(
                "shadow observation ID does not match deterministic identity"
            )

        opportunity = session.get(PublicStructureFlowOpportunityRow, write.opportunity_id)
        if opportunity is None:
            raise PublicStructureFlowConflictError("opportunity does not exist")
        if (
            opportunity.campaign_id != write.campaign_id
            or opportunity.market != write.market.value
            or opportunity.symbol != write.symbol.upper()
            or opportunity.event_time_ms != write.event_time_ms
        ):
            raise PublicStructureFlowConflictError(
                "recorded opportunity identity does not match terminal write"
            )

        stable_payload = _stable_shadow_payload(write.payload)
        payload_json = _canonical_json(stable_payload)
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        existing = session.get(PublicStructureFlowShadowRow, write.observation_id)
        existing_ready_at: int | None = None
        if existing is not None:
            try:
                existing_value = json.loads(existing.payload_json)
            except json.JSONDecodeError as exc:  # pragma: no cover
                raise PublicStructureFlowConflictError(
                    "existing shadow payload is invalid JSON"
                ) from exc
            if not isinstance(existing_value, dict):  # pragma: no cover
                raise PublicStructureFlowConflictError("existing shadow payload is not an object")
            legacy_ready = existing_value.get("ready_at_ms")
            if isinstance(legacy_ready, int):
                existing_ready_at = legacy_ready
            existing_stable_json = _canonical_json(
                _stable_shadow_payload(cast(dict[str, object], existing_value))
            )
            if (
                existing.campaign_id != write.campaign_id
                or existing.market != write.market.value
                or existing.symbol != write.symbol.upper()
                or existing.timeframe != write.timeframe
                or existing.event_time_ms != write.event_time_ms
                or existing.candidate_version != write.candidate_version
                or existing_stable_json != payload_json
            ):
                raise PublicStructureFlowConflictError(
                    f"observation ID {write.observation_id} maps to conflicting research payloads"
                )
        else:
            session.add(
                PublicStructureFlowShadowRow(
                    observation_id=write.observation_id,
                    campaign_id=write.campaign_id,
                    market=write.market.value,
                    symbol=write.symbol.upper(),
                    timeframe=write.timeframe,
                    event_time_ms=write.event_time_ms,
                    candidate_version=write.candidate_version,
                    payload_json=payload_json,
                    payload_sha256=payload_sha256,
                    recorded_at_ms=write.recorded_at_ms,
                )
            )

        canonical_ready_at = (
            write.ready_at_ms
            if write.reason == "conflicting_1m_evidence"
            else opportunity.ready_at_ms
            if opportunity.ready_at_ms is not None
            else existing_ready_at
            if existing_ready_at is not None
            else write.ready_at_ms
        )
        if opportunity.status != "PENDING":
            if (
                opportunity.status == write.status
                and opportunity.reason == write.reason
                and opportunity.observation_id == write.observation_id
            ):
                return False
            opportunity.conflict_count += 1
            raise PublicStructureFlowConflictError(
                f"opportunity {write.opportunity_id} is already terminal with different content"
            )
        opportunity.status = write.status
        opportunity.reason = write.reason
        opportunity.observation_id = write.observation_id
        opportunity.ready_at_ms = canonical_ready_at
        opportunity.updated_at_ms = max(opportunity.updated_at_ms, write.recorded_at_ms)
        return existing is None

    def commit_public_structure_flow_result(
        self,
        write: PublicStructureFlowTerminalWrite,
    ) -> bool:
        """Atomically append one research result and terminalize its denominator."""

        with Session(self.engine) as session:
            inserted = self._apply_public_structure_flow_terminal_write(session, write)
            session.commit()
            return inserted

    def register_public_structure_flow_opportunity(
        self,
        *,
        opportunity_id: str,
        campaign_id: str,
        market: Market,
        symbol: str,
        event_time_ms: int,
        deadline_ms: int,
        evidence: dict[str, object],
        created_at_ms: int,
        eviction_write: PublicStructureFlowTerminalWrite | None = None,
    ) -> bool:
        """Create one durable denominator row before a shadow result is finalized."""

        evidence_json = json.dumps(
            evidence,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        evidence_sha256 = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            campaign = session.get(PublicStructureFlowCampaignRow, campaign_id)
            if campaign is None:
                raise PublicStructureFlowConflictError(
                    f"unregistered public structure-flow campaign: {campaign_id}"
                )
            if event_time_ms < campaign.activation_ms:
                raise PublicStructureFlowConflictError(
                    "opportunity event precedes registered campaign activation"
                )
            if created_at_ms < event_time_ms:
                raise PublicStructureFlowConflictError(
                    "opportunity creation time precedes event time"
                )
            if deadline_ms < event_time_ms:
                raise PublicStructureFlowConflictError("opportunity deadline precedes event time")
            expected_id = hashlib.sha256(
                "\0".join((campaign_id, market.value, symbol.upper(), str(event_time_ms))).encode(
                    "utf-8"
                )
            ).hexdigest()[:32]
            if opportunity_id != expected_id:
                raise PublicStructureFlowConflictError(
                    "opportunity ID does not match deterministic identity"
                )
            expected_evidence = {
                "campaign_id": campaign_id,
                "candidate_version": campaign.candidate_version,
                "market": market.value,
                "symbol": symbol.upper(),
                "event_time_ms": event_time_ms,
                "deadline_ms": deadline_ms,
                "contract_sha256": campaign.contract_sha256,
                "manifest_sha256": campaign.manifest_sha256,
            }
            for key, expected in expected_evidence.items():
                if evidence.get(key) != expected:
                    raise PublicStructureFlowConflictError(
                        f"opportunity evidence field {key} does not match repository authority"
                    )
            existing = session.get(PublicStructureFlowOpportunityRow, opportunity_id)
            if existing is not None:
                if (
                    existing.campaign_id != campaign_id
                    or existing.market != market.value
                    or existing.symbol != symbol.upper()
                    or existing.event_time_ms != event_time_ms
                    or existing.deadline_ms != deadline_ms
                    or existing.evidence_sha256 != evidence_sha256
                    or existing.evidence_json != evidence_json
                ):
                    existing.conflict_count += 1
                    session.commit()
                    raise PublicStructureFlowConflictError(
                        f"opportunity ID {opportunity_id} maps to conflicting evidence"
                    )
                existing.duplicate_count += 1
                existing.updated_at_ms = max(existing.updated_at_ms, created_at_ms)
                session.commit()
                return False
            if eviction_write is not None:
                self._apply_public_structure_flow_terminal_write(session, eviction_write)
            session.add(
                PublicStructureFlowOpportunityRow(
                    opportunity_id=opportunity_id,
                    campaign_id=campaign_id,
                    market=market.value,
                    symbol=symbol.upper(),
                    event_time_ms=event_time_ms,
                    deadline_ms=deadline_ms,
                    status="PENDING",
                    reason=None,
                    evidence_json=evidence_json,
                    evidence_sha256=evidence_sha256,
                    observation_id=None,
                    created_at_ms=created_at_ms,
                    updated_at_ms=created_at_ms,
                    ready_at_ms=None,
                    duplicate_count=0,
                    late_event_count=0,
                    conflict_count=0,
                )
            )
            session.commit()
            return True

    def register_public_structure_flow_missing_opportunity(
        self,
        *,
        opportunity_id: str,
        campaign_id: str,
        market: Market,
        symbol: str,
        event_time_ms: int,
        deadline_ms: int,
        evidence: dict[str, object],
        ready_at_ms: int,
        recorded_at_ms: int,
        reason: str,
    ) -> bool:
        """Atomically register and terminalize one denominator that has no computable result."""

        evidence_json = _canonical_json(evidence)
        evidence_sha256 = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            campaign = session.get(PublicStructureFlowCampaignRow, campaign_id)
            if campaign is None:
                raise PublicStructureFlowConflictError(
                    f"unregistered public structure-flow campaign: {campaign_id}"
                )
            if event_time_ms < campaign.activation_ms:
                raise PublicStructureFlowConflictError(
                    "opportunity event precedes registered campaign activation"
                )
            if recorded_at_ms < event_time_ms:
                raise PublicStructureFlowConflictError(
                    "missing opportunity record time precedes event time"
                )
            if ready_at_ms < event_time_ms or ready_at_ms > recorded_at_ms:
                raise PublicStructureFlowConflictError(
                    "missing opportunity ready time is outside event/record bounds"
                )
            expected_id = hashlib.sha256(
                "\0".join((campaign_id, market.value, symbol.upper(), str(event_time_ms))).encode(
                    "utf-8"
                )
            ).hexdigest()[:32]
            if opportunity_id != expected_id:
                raise PublicStructureFlowConflictError(
                    "opportunity ID does not match deterministic identity"
                )
            expected_evidence = {
                "campaign_id": campaign_id,
                "candidate_version": campaign.candidate_version,
                "market": market.value,
                "symbol": symbol.upper(),
                "event_time_ms": event_time_ms,
                "deadline_ms": deadline_ms,
                "contract_sha256": campaign.contract_sha256,
                "manifest_sha256": campaign.manifest_sha256,
                "missing_reason": reason,
            }
            for key, expected in expected_evidence.items():
                if evidence.get(key) != expected:
                    raise PublicStructureFlowConflictError(
                        f"missing opportunity evidence field {key} does not match authority"
                    )
            existing = session.get(PublicStructureFlowOpportunityRow, opportunity_id)
            if existing is not None:
                if (
                    existing.campaign_id != campaign_id
                    or existing.market != market.value
                    or existing.symbol != symbol.upper()
                    or existing.event_time_ms != event_time_ms
                    or existing.deadline_ms != deadline_ms
                    or existing.evidence_sha256 != evidence_sha256
                    or existing.evidence_json != evidence_json
                ):
                    existing.conflict_count += 1
                    session.commit()
                    raise PublicStructureFlowConflictError(
                        f"opportunity ID {opportunity_id} maps to conflicting evidence"
                    )
                if existing.status == "MISSING" and existing.reason == reason:
                    existing.duplicate_count += 1
                    existing.updated_at_ms = max(existing.updated_at_ms, recorded_at_ms)
                    session.commit()
                    return False
                if existing.status != "PENDING":
                    existing.conflict_count += 1
                    session.commit()
                    raise PublicStructureFlowConflictError(
                        f"opportunity {opportunity_id} is already terminal with different content"
                    )
                existing.status = "MISSING"
                existing.reason = reason
                existing.observation_id = None
                existing.ready_at_ms = existing.ready_at_ms or ready_at_ms
                existing.updated_at_ms = max(existing.updated_at_ms, recorded_at_ms)
                session.commit()
                return False
            session.add(
                PublicStructureFlowOpportunityRow(
                    opportunity_id=opportunity_id,
                    campaign_id=campaign_id,
                    market=market.value,
                    symbol=symbol.upper(),
                    event_time_ms=event_time_ms,
                    deadline_ms=deadline_ms,
                    status="MISSING",
                    reason=reason,
                    evidence_json=evidence_json,
                    evidence_sha256=evidence_sha256,
                    observation_id=None,
                    created_at_ms=ready_at_ms,
                    updated_at_ms=recorded_at_ms,
                    ready_at_ms=ready_at_ms,
                    duplicate_count=0,
                    late_event_count=0,
                    conflict_count=0,
                )
            )
            session.commit()
            return True

    def update_public_structure_flow_pending_evidence(
        self,
        *,
        opportunity_id: str,
        evidence: dict[str, object],
        updated_at_ms: int,
        late_event: bool = False,
        evidence_ready_at_ms: int | None = None,
    ) -> bool:
        """Replace evidence only while the denominator row is still pending."""

        evidence_json = json.dumps(
            evidence,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        evidence_sha256 = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        with Session(self.engine) as session:
            row = session.get(PublicStructureFlowOpportunityRow, opportunity_id)
            if row is None:
                raise PublicStructureFlowConflictError("pending opportunity does not exist")
            if row.status != "PENDING":
                return False
            previous = json.loads(row.evidence_json)
            if not isinstance(previous, dict):  # pragma: no cover
                raise PublicStructureFlowConflictError("pending evidence is not an object")
            mutable_keys = {
                "minute_candles",
                "c1_forced_unavailable_reason",
                "c1_forced_unavailable_at_ms",
            }
            previous_frozen = {
                key: value for key, value in previous.items() if key not in mutable_keys
            }
            updated_frozen = {
                key: value for key, value in evidence.items() if key not in mutable_keys
            }
            if previous_frozen != updated_frozen:
                row.conflict_count += 1
                session.commit()
                raise PublicStructureFlowConflictError(
                    "pending update attempted to change frozen event-time evidence"
                )
            evidence_changed = row.evidence_json != evidence_json
            previous_ready_at_ms = row.ready_at_ms
            row.evidence_json = evidence_json
            row.evidence_sha256 = evidence_sha256
            row.updated_at_ms = max(row.updated_at_ms, updated_at_ms)
            if evidence_ready_at_ms is not None:
                if evidence_ready_at_ms < row.event_time_ms:
                    raise PublicStructureFlowConflictError(
                        "evidence ready time precedes opportunity event"
                    )
                row.ready_at_ms = (
                    evidence_ready_at_ms
                    if row.ready_at_ms is None
                    else min(row.ready_at_ms, evidence_ready_at_ms)
                )
            ready_changed = row.ready_at_ms != previous_ready_at_ms
            if late_event and (evidence_changed or ready_changed):
                row.late_event_count += 1
            session.commit()
            return True

    def finalize_public_structure_flow_opportunity(
        self,
        *,
        opportunity_id: str,
        status: str,
        reason: str | None,
        observation_id: str | None,
        ready_at_ms: int,
    ) -> bool:
        allowed = {"RECORDED", "RECORDED_DEGRADED", "MISSING"}
        if status not in allowed:
            raise ValueError(f"unsupported public structure-flow terminal status: {status}")
        with Session(self.engine) as session:
            row = session.get(PublicStructureFlowOpportunityRow, opportunity_id)
            if row is None:
                raise PublicStructureFlowConflictError("opportunity does not exist")
            if ready_at_ms < row.event_time_ms:
                raise PublicStructureFlowConflictError("ready time precedes opportunity event")
            if status == "MISSING":
                if observation_id is not None:
                    raise PublicStructureFlowConflictError(
                        "missing opportunity must not reference an observation"
                    )
            else:
                if observation_id is None:
                    raise PublicStructureFlowConflictError(
                        "recorded opportunity requires an observation ID"
                    )
                observation = session.get(PublicStructureFlowShadowRow, observation_id)
                if observation is None:
                    raise PublicStructureFlowConflictError(
                        "recorded opportunity references a missing observation"
                    )
                if (
                    observation.campaign_id != row.campaign_id
                    or observation.market != row.market
                    or observation.symbol != row.symbol
                    or observation.event_time_ms != row.event_time_ms
                ):
                    raise PublicStructureFlowConflictError(
                        "recorded opportunity observation identity does not match denominator"
                    )
            if row.status != "PENDING":
                if (
                    row.status == status
                    and row.reason == reason
                    and row.observation_id == observation_id
                ):
                    return False
                row.conflict_count += 1
                session.commit()
                raise PublicStructureFlowConflictError(
                    f"opportunity {opportunity_id} is already terminal with different content"
                )
            row.status = status
            row.reason = reason
            row.observation_id = observation_id
            row.ready_at_ms = ready_at_ms
            row.updated_at_ms = max(row.updated_at_ms, ready_at_ms)
            session.commit()
            return True

    def load_public_structure_flow_pending(
        self, campaign_id: str, *, limit: int = 128
    ) -> list[dict[str, object]]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(PublicStructureFlowOpportunityRow)
                .where(
                    PublicStructureFlowOpportunityRow.campaign_id == campaign_id,
                    PublicStructureFlowOpportunityRow.status == "PENDING",
                )
                .order_by(PublicStructureFlowOpportunityRow.event_time_ms)
                .limit(limit)
            ).all()
            return [
                {
                    "opportunity_id": row.opportunity_id,
                    "campaign_id": row.campaign_id,
                    "market": row.market,
                    "symbol": row.symbol,
                    "event_time_ms": row.event_time_ms,
                    "deadline_ms": row.deadline_ms,
                    "status": row.status,
                    "reason": row.reason,
                    "evidence": cast(dict[str, object], json.loads(row.evidence_json)),
                    "evidence_sha256": row.evidence_sha256,
                    "created_at_ms": row.created_at_ms,
                    "updated_at_ms": row.updated_at_ms,
                    "ready_at_ms": row.ready_at_ms,
                    "duplicate_count": row.duplicate_count,
                    "late_event_count": row.late_event_count,
                    "conflict_count": row.conflict_count,
                }
                for row in rows
            ]

    def load_public_structure_flow_opportunity(
        self, opportunity_id: str
    ) -> dict[str, object] | None:
        """Load one denominator row for crash/acknowledgement reconciliation."""

        with Session(self.engine) as session:
            row = session.get(PublicStructureFlowOpportunityRow, opportunity_id)
            if row is None:
                return None
            return {
                "opportunity_id": row.opportunity_id,
                "campaign_id": row.campaign_id,
                "market": row.market,
                "symbol": row.symbol,
                "event_time_ms": row.event_time_ms,
                "deadline_ms": row.deadline_ms,
                "status": row.status,
                "reason": row.reason,
                "evidence": cast(dict[str, object], json.loads(row.evidence_json)),
                "evidence_sha256": row.evidence_sha256,
                "observation_id": row.observation_id,
                "created_at_ms": row.created_at_ms,
                "updated_at_ms": row.updated_at_ms,
                "ready_at_ms": row.ready_at_ms,
                "duplicate_count": row.duplicate_count,
                "late_event_count": row.late_event_count,
                "conflict_count": row.conflict_count,
            }

    def summarize_public_structure_flow_campaign(self, campaign_id: str) -> dict[str, object]:
        """Return read-only engineering counts; never labels them as trading performance."""

        with Session(self.engine) as session:
            campaign = session.get(PublicStructureFlowCampaignRow, campaign_id)
            if campaign is None:
                raise ValueError(f"unknown public structure-flow campaign: {campaign_id}")
            opportunity = PublicStructureFlowOpportunityRow
            observation = PublicStructureFlowShadowRow
            expected_opportunities = int(
                session.scalar(
                    select(func.count()).select_from(opportunity).where(
                        opportunity.campaign_id == campaign_id
                    )
                )
                or 0
            )
            actual_observations = int(
                session.scalar(
                    select(func.count()).select_from(observation).where(
                        observation.campaign_id == campaign_id
                    )
                )
                or 0
            )
            opportunity_status = {
                str(status): int(count)
                for status, count in session.execute(
                    select(opportunity.status, func.count())
                    .where(opportunity.campaign_id == campaign_id)
                    .group_by(opportunity.status)
                )
            }
            missing_reasons = {
                str(reason): int(count)
                for reason, count in session.execute(
                    select(opportunity.reason, func.count())
                    .where(
                        opportunity.campaign_id == campaign_id,
                        opportunity.reason.is_not(None),
                    )
                    .group_by(opportunity.reason)
                )
                if reason is not None
            }
            duplicates, late_events, conflicts = (
                int(value or 0)
                for value in session.execute(
                    select(
                        func.sum(opportunity.duplicate_count),
                        func.sum(opportunity.late_event_count),
                        func.sum(opportunity.conflict_count),
                    ).where(opportunity.campaign_id == campaign_id)
                ).one()
            )

            event_observation = aliased(PublicStructureFlowShadowRow)
            event_join = (
                (event_observation.campaign_id == opportunity.campaign_id)
                & (event_observation.market == opportunity.market)
                & (event_observation.symbol == opportunity.symbol)
                & (event_observation.event_time_ms == opportunity.event_time_ms)
            )
            unobserved_opportunities = int(
                session.scalar(
                    select(func.count())
                    .select_from(opportunity)
                    .outerjoin(event_observation, event_join)
                    .where(
                        opportunity.campaign_id == campaign_id,
                        event_observation.observation_id.is_(None),
                    )
                )
                or 0
            )
            pending_with_observation = int(
                session.scalar(
                    select(func.count())
                    .select_from(opportunity)
                    .join(event_observation, event_join)
                    .where(
                        opportunity.campaign_id == campaign_id,
                        opportunity.status == "PENDING",
                    )
                )
                or 0
            )
            missing_with_observation = int(
                session.scalar(
                    select(func.count())
                    .select_from(opportunity)
                    .join(event_observation, event_join)
                    .where(
                        opportunity.campaign_id == campaign_id,
                        opportunity.status == "MISSING",
                    )
                )
                or 0
            )
            linked_observation = aliased(PublicStructureFlowShadowRow)
            recorded_without_link = int(
                session.scalar(
                    select(func.count())
                    .select_from(opportunity)
                    .outerjoin(
                        linked_observation,
                        linked_observation.observation_id == opportunity.observation_id,
                    )
                    .where(
                        opportunity.campaign_id == campaign_id,
                        opportunity.status.in_(("RECORDED", "RECORDED_DEGRADED")),
                        linked_observation.observation_id.is_(None),
                    )
                )
                or 0
            )
            event_opportunity = aliased(PublicStructureFlowOpportunityRow)
            observation_join = (
                (event_opportunity.campaign_id == observation.campaign_id)
                & (event_opportunity.market == observation.market)
                & (event_opportunity.symbol == observation.symbol)
                & (event_opportunity.event_time_ms == observation.event_time_ms)
            )
            orphan_observations = int(
                session.scalar(
                    select(func.count())
                    .select_from(observation)
                    .outerjoin(event_opportunity, observation_join)
                    .where(
                        observation.campaign_id == campaign_id,
                        event_opportunity.opportunity_id.is_(None),
                    )
                )
                or 0
            )
            candidate_status: dict[str, dict[str, int]] = {}
            directions: dict[str, dict[str, int]] = {}
            input_quality: dict[str, int] = {}
            payload_stream = session.scalars(
                select(observation.payload_json)
                .where(observation.campaign_id == campaign_id)
                .execution_options(yield_per=500)
            )
            for payload_json in payload_stream:
                payload = json.loads(payload_json)
                if not isinstance(payload, dict):
                    continue
                quality = payload.get("input_quality")
                if isinstance(quality, str):
                    input_quality[quality] = input_quality.get(quality, 0) + 1
                candidates = payload.get("candidates")
                if not isinstance(candidates, list):
                    continue
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    candidate_id = candidate.get("candidate_id")
                    status = candidate.get("status")
                    direction = candidate.get("direction")
                    if isinstance(candidate_id, str) and isinstance(status, str):
                        by_status = candidate_status.setdefault(candidate_id, {})
                        by_status[status] = by_status.get(status, 0) + 1
                    if isinstance(candidate_id, str) and isinstance(direction, str):
                        by_direction = directions.setdefault(candidate_id, {})
                        by_direction[direction] = by_direction.get(direction, 0) + 1
            return {
                "campaign_id": campaign_id,
                "candidate_version": campaign.candidate_version,
                "contract_sha256": campaign.contract_sha256,
                "manifest_sha256": campaign.manifest_sha256,
                "expected_opportunities": expected_opportunities,
                "actual_observations": actual_observations,
                "unobserved_opportunities": unobserved_opportunities,
                "orphan_observations": orphan_observations,
                "pending_with_observation": pending_with_observation,
                "missing_with_observation": missing_with_observation,
                "recorded_without_linked_observation": recorded_without_link,
                "terminal_inconsistencies": (
                    pending_with_observation
                    + missing_with_observation
                    + recorded_without_link
                    + orphan_observations
                ),
                "opportunity_status": opportunity_status,
                "missing_reasons": missing_reasons,
                "candidate_status": candidate_status,
                "directions": directions,
                "input_quality": input_quality,
                "duplicate_attempts": duplicates,
                "late_events": late_events,
                "expired_opportunities": missing_reasons.get("minute_deadline_expired", 0),
                "capacity_evictions": missing_reasons.get("pending_capacity_limit", 0),
                "symbol_exit_finalizations": missing_reasons.get(
                    "symbol_left_data_universe", 0
                ),
                "conflicts": conflicts,
            }

    def load_public_structure_flow_shadow(
        self, observation_id: str
    ) -> dict[str, object] | None:
        with Session(self.engine) as session:
            row = session.get(PublicStructureFlowShadowRow, observation_id)
            if row is None:
                return None
            value = json.loads(row.payload_json)
            if not isinstance(value, dict):  # pragma: no cover - write path guarantees object
                raise ValueError("public structure-flow payload is not an object")
            return cast(dict[str, object], value)

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
        observation: RuleEvaluation | None = None,
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
            if observation is not None:
                episode = self._load_market_watch_episode(session, observation)
                if observation.event_time_ms <= episode.last_event_ms:
                    return False
                emit = not episode.active and observation.event_time_ms >= episode.cooldown_until_ms
                episode.active = True
                episode.last_event_ms = observation.event_time_ms
                if not emit:
                    session.commit()
                    return False
                episode.cooldown_until_ms = observation.event_time_ms + int(
                    observation.metadata["event_cooldown_seconds"]
                ) * 1000
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
                    .where(AlertOutboxRow.status.in_(("pending", "sending", "uncertain")))
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

    @staticmethod
    def _episode_key(evaluation: RuleEvaluation) -> tuple[str, str, str, str]:
        return (
            evaluation.market.value, evaluation.symbol,
            evaluation.family.value, evaluation.timeframe,
        )

    def _load_market_watch_episode(
        self, session: Session, evaluation: RuleEvaluation,
    ) -> MarketWatchEpisodeRow:
        row = session.get(MarketWatchEpisodeRow, self._episode_key(evaluation))
        if row is not None:
            return row
        # Include pending/uncertain delivery intents when upgrading the old release.
        last_intent = session.scalar(
            select(func.max(SignalRow.event_time_ms))
            .join(AlertOutboxRow, SignalRow.event_id == AlertOutboxRow.event_id)
            .where(
                SignalRow.market == evaluation.market.value,
                SignalRow.symbol == evaluation.symbol,
                SignalRow.family == evaluation.family.value,
                SignalRow.timeframe == evaluation.timeframe,
                SignalRow.event_time_ms >= evaluation.event_time_ms
                - int(evaluation.metadata["event_cooldown_seconds"]) * 1000,
                AlertOutboxRow.status != "disabled",
            )
        )
        row = MarketWatchEpisodeRow(
            market=evaluation.market.value, symbol=evaluation.symbol,
            family=evaluation.family.value, timeframe=evaluation.timeframe,
            active=(last_intent is not None and int(last_intent)
                    + int(evaluation.metadata["event_cooldown_seconds"]) * 1000
                    > evaluation.event_time_ms),
            last_event_ms=int(last_intent) if last_intent is not None else -1,
            cooldown_until_ms=(
                int(last_intent) + int(evaluation.metadata["event_cooldown_seconds"]) * 1000
                if last_intent is not None else 0
            ),
        )
        session.add(row)
        return row

    def rearm_market_watch(self, evaluation: RuleEvaluation) -> None:
        """Persist a false observation, without rewinding a newer event."""
        with Session(self.engine) as session:
            row = self._load_market_watch_episode(session, evaluation)
            if evaluation.event_time_ms > row.last_event_ms:
                row.active = False
                row.last_event_ms = evaluation.event_time_ms
                session.commit()

    def prune_market_watch_episodes(
        self, market: Market, active_symbols: frozenset[str], now_ms: int
    ) -> None:
        """Re-arm departing symbols, keep cooldowns, prune records older than 48h."""
        with Session(self.engine) as session:
            rows = session.scalars(
                select(MarketWatchEpisodeRow).where(MarketWatchEpisodeRow.market == market.value)
            ).all()
            for row in rows:
                if row.last_event_ms < now_ms - 48 * 3_600_000:
                    session.delete(row)
                    continue
                if row.symbol not in active_symbols:
                    row.active = False
                    if row.membership_end_ms is None:
                        # Stamp an exit once, so repeated refreshes cannot defeat pruning.
                        row.last_event_ms = max(row.last_event_ms, now_ms)
                        row.membership_end_ms = now_ms
                else:
                    row.membership_end_ms = None
            session.commit()

    def update_gainer_watch(
        self,
        market: Market,
        gainers: tuple[Instrument, ...],
        valid_symbols: frozenset[str],
        observed_ms: int,
        *,
        qualifying_quote_volume: float,
        retention_ms: int,
        maximum_retained: int,
    ) -> tuple[GainerWatchState, ...]:
        """Update one successful ranking snapshot; cap eviction ends qualification."""
        with Session(self.engine) as session:
            rows = session.scalars(
                select(GainerWatchRow).where(GainerWatchRow.market == market.value)
            ).all()
            live: dict[str, GainerWatchRow] = {}
            for row in rows:
                if row.expires_at_ms <= observed_ms or row.symbol not in valid_symbols:
                    session.delete(row)
                else:
                    live[row.symbol] = row
            current = {item.symbol for item in gainers}
            for item in gainers:
                row = live.get(item.symbol)
                volume = float(item.quote_volume)
                if row is None:
                    if volume < qualifying_quote_volume:
                        continue
                    row = GainerWatchRow(
                        market=market.value, symbol=item.symbol,
                        last_top5_observed_ms=observed_ms,
                        expires_at_ms=observed_ms + retention_ms,
                        peak_quote_volume_usdt=volume,
                    )
                    session.add(row)
                    live[item.symbol] = row
                if observed_ms >= row.last_top5_observed_ms:
                    row.last_top5_observed_ms = observed_ms
                    row.expires_at_ms = observed_ms + retention_ms
                    row.peak_quote_volume_usdt = max(row.peak_quote_volume_usdt, volume)
            alumni = sorted(
                (row for row in live.values() if row.symbol not in current),
                key=lambda row: (-row.last_top5_observed_ms, row.symbol),
            )
            for row in alumni[maximum_retained:]:
                session.delete(row)
                del live[row.symbol]
            result = tuple(
                GainerWatchState(
                    market=market, symbol=row.symbol,
                    last_top5_observed_ms=row.last_top5_observed_ms,
                    expires_at_ms=row.expires_at_ms,
                    peak_quote_volume_usdt=row.peak_quote_volume_usdt,
                )
                for row in sorted(live.values(), key=lambda row: row.symbol)
            )
            session.commit()
            return result

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

    def latest_delivered_signal_time(
        self, market: Market, symbol: str, family: SignalFamily
    ) -> int | None:
        """Find the last delivered event for a bounded market-watch cooldown."""

        with Session(self.engine) as session:
            return session.scalar(
                select(func.max(SignalRow.event_time_ms))
                .join(AlertOutboxRow, AlertOutboxRow.event_id == SignalRow.event_id)
                .where(
                    SignalRow.market == market.value,
                    SignalRow.symbol == symbol.upper(),
                    SignalRow.family == family.value,
                    AlertOutboxRow.status == "delivered",
                )
            )

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
