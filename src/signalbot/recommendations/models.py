from __future__ import annotations

import hashlib
from decimal import Decimal
from enum import StrEnum

from pydantic import Field, field_validator, model_validator

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import FrozenModel


class RecommendationAction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_ENTRY = "NO_ENTRY"


class RecommendationKind(StrEnum):
    ENTRY_CANDIDATE = "ENTRY_CANDIDATE"
    EXIT_WARNING = "EXIT_WARNING"
    RISK_WARNING = "RISK_WARNING"
    HOLD = "HOLD"


class ScoreKind(StrEnum):
    RULE_STRENGTH = "RULE_STRENGTH"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class EvidenceTier(StrEnum):
    UNVALIDATED = "UNVALIDATED"
    HISTORICAL_ONLY = "HISTORICAL_ONLY"
    PROSPECTIVE_SHADOW = "PROSPECTIVE_SHADOW"
    PROMOTION_ELIGIBLE = "PROMOTION_ELIGIBLE"


class RecommendationEnvelope(FrozenModel):
    """Read-only, deterministic projection of one signal decision."""

    event_id: str
    source_event_id: str
    action: RecommendationAction
    kind: RecommendationKind
    market: Market
    symbol: str
    source_family: SignalFamily
    source_stage: SignalStage
    timeframe: str
    decision_time_ms: int = Field(ge=0)
    expires_at_ms: int = Field(ge=0)
    reasons: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    invalidation: Decimal | None = None
    evidence_strength: int | None = Field(default=None, ge=0, le=100)
    score_kind: ScoreKind = ScoreKind.RULE_STRENGTH
    evidence_tier: EvidenceTier = EvidenceTier.UNVALIDATED
    rule_version: str

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_semantics(self) -> RecommendationEnvelope:
        if not self.event_id.strip() or not self.source_event_id.strip():
            raise ValueError("recommendation identities must be non-empty")
        if not self.symbol.strip():
            raise ValueError("recommendation symbol must be non-empty")
        if self.expires_at_ms < self.decision_time_ms:
            raise ValueError("recommendation expiry must not precede decision time")
        if self.kind is RecommendationKind.ENTRY_CANDIDATE:
            if self.action is RecommendationAction.NO_ENTRY:
                raise ValueError("NO_ENTRY cannot be an entry candidate")
            if self.market is Market.SPOT and self.action is RecommendationAction.SHORT:
                raise ValueError("Spot short direction cannot be a short entry")
        if self.kind is RecommendationKind.RISK_WARNING:
            if self.action is not RecommendationAction.NO_ENTRY:
                raise ValueError("risk warnings cannot be directional entries")
        if self.action is RecommendationAction.NO_ENTRY and not self.blockers:
            raise ValueError("NO_ENTRY requires at least one blocker")
        if self.score_kind is ScoreKind.NOT_APPLICABLE and self.evidence_strength is not None:
            raise ValueError("NOT_APPLICABLE scores must not carry evidence strength")
        if self.score_kind is ScoreKind.RULE_STRENGTH and self.evidence_strength is None:
            raise ValueError("RULE_STRENGTH scores require evidence strength")
        return self

    @property
    def is_actionable(self) -> bool:
        return self.kind is RecommendationKind.ENTRY_CANDIDATE

    def is_expired(self, as_of_ms: int) -> bool:
        if as_of_ms < 0:
            raise ValueError("as_of_ms must be non-negative")
        return as_of_ms >= self.expires_at_ms

    @staticmethod
    def deterministic_event_id(source_event_id: str, projection_version: str) -> str:
        if not source_event_id.strip() or not projection_version.strip():
            raise ValueError("event identity inputs must be non-empty")
        identity = f"recommendation|{source_event_id}|{projection_version}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def direction_action(direction: Direction) -> RecommendationAction:
    if direction is Direction.LONG:
        return RecommendationAction.LONG
    if direction is Direction.SHORT:
        return RecommendationAction.SHORT
    raise ValueError("risk directions do not map to directional recommendations")
