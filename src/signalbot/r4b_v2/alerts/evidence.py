from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from signalbot.r4b_v2.strategy.evidence_score import (
    EvidenceReadinessV2,
    EvidenceScoreDecisionV2,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_KST = ZoneInfo("Asia/Seoul")
_HEADLINE_PREFIX = "Evidence Score (not a probability) — "
_FIXED_DISCLAIMER = (
    "Evidence agreement only; not a probability, trade instruction, "
    "or profitability claim. Primary A/B/C decision is unchanged."
)


class EvidenceAlertContractErrorV2(ValueError):
    """Raised when a shadow annotation cannot be rendered safely."""


@dataclass(frozen=True, slots=True)
class EvidenceAlertAnnotationV2:
    """A decision-bound annotation whose public text cannot be caller-injected."""

    source_decision: EvidenceScoreDecisionV2 = field(repr=False)
    event_id: str = field(init=False)
    source_payload_sha256: str = field(init=False)
    headline: str = field(init=False)
    score_text: str = field(init=False)
    family_count_text: str = field(init=False)
    family_evidence_lines: tuple[str, ...] = field(init=False)
    status_text: str = field(init=False)
    time_text: str = field(init=False)
    disclaimer: str = field(init=False, default=_FIXED_DISCLAIMER)
    invalidation: str = field(init=False)
    rule_version: str = field(init=False)
    role: str = field(init=False)
    standalone_alert: bool = field(init=False, default=False)
    may_suppress_primary_alert: bool = field(init=False, default=False)
    may_change_primary_decision: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        decision = self.source_decision
        if not isinstance(decision, EvidenceScoreDecisionV2):
            raise EvidenceAlertContractErrorV2(
                "source_decision must be an EvidenceScoreDecisionV2"
            )
        family_lines = tuple(
            (
                f"{item.family.value}: {_direction_label(item.direction)} "
                f"strength={_unsigned_micros(item.strength_micros)} "
                f"status={item.readiness.value}"
            )
            for item in decision.observations
        )
        if decision.status is EvidenceReadinessV2.READY:
            assert decision.evidence_score_micros is not None
            assert decision.bullish_family_count is not None
            assert decision.bearish_family_count is not None
            assert decision.neutral_family_count is not None
            score_text = (
                "agreement="
                f"{_signed_score_100(decision.evidence_score_micros)}/100"
            )
            family_count_text = (
                f"bullish={decision.bullish_family_count} | "
                f"bearish={decision.bearish_family_count} | "
                f"neutral={decision.neutral_family_count}"
            )
        else:
            score_text = "UNAVAILABLE"
            family_count_text = (
                "WITHHELD_BECAUSE_AT_LEAST_ONE_FAMILY_IS_NOT_READY"
            )
        object.__setattr__(self, "event_id", decision.event_id)
        object.__setattr__(
            self,
            "source_payload_sha256",
            decision.payload_sha256,
        )
        object.__setattr__(
            self,
            "headline",
            f"{_HEADLINE_PREFIX}{decision.bias.value}",
        )
        object.__setattr__(self, "score_text", score_text)
        object.__setattr__(self, "family_count_text", family_count_text)
        object.__setattr__(self, "family_evidence_lines", family_lines)
        object.__setattr__(self, "status_text", decision.status.value)
        object.__setattr__(
            self,
            "time_text",
            format_evidence_times_v2(decision.bar_close_ms),
        )
        object.__setattr__(self, "invalidation", decision.invalidation)
        object.__setattr__(self, "rule_version", decision.rule_version)
        object.__setattr__(self, "role", decision.role)


def render_evidence_alert_annotation_v2(
    decision: EvidenceScoreDecisionV2,
) -> EvidenceAlertAnnotationV2:
    """Render deterministic decision-bound fields without probability language."""

    if not isinstance(decision, EvidenceScoreDecisionV2):
        raise EvidenceAlertContractErrorV2(
            "decision must be an EvidenceScoreDecisionV2"
        )
    return EvidenceAlertAnnotationV2(source_decision=decision)


def format_evidence_times_v2(timestamp_ms: int) -> str:
    """Render one Unix-millisecond timestamp in the two required alert zones."""

    if type(timestamp_ms) is not int or timestamp_ms < 0:
        raise EvidenceAlertContractErrorV2(
            "timestamp_ms must be a nonnegative integer"
        )
    utc = _EPOCH + timedelta(milliseconds=timestamp_ms)
    kst = utc.astimezone(_KST)
    return f"UTC {utc:%Y-%m-%d %H:%M:%S.%f} | KST {kst:%Y-%m-%d %H:%M:%S.%f}"


def _direction_label(direction: int) -> str:
    if direction == 1:
        return "BULLISH"
    if direction == -1:
        return "BEARISH"
    if direction == 0:
        return "NEUTRAL"
    raise EvidenceAlertContractErrorV2("unsupported evidence direction")


def _signed_score_100(value_micros: int) -> str:
    if type(value_micros) is not int or not -1_000_000 <= value_micros <= 1_000_000:
        raise EvidenceAlertContractErrorV2(
            "evidence score micros must be an integer in [-1000000, 1000000]"
        )
    sign = "+" if value_micros >= 0 else "-"
    absolute = abs(value_micros)
    whole, fraction = divmod(absolute, 10_000)
    return f"{sign}{whole}.{fraction:04d}"


def _unsigned_micros(value_micros: int) -> str:
    if type(value_micros) is not int or not 0 <= value_micros <= 1_000_000:
        raise EvidenceAlertContractErrorV2(
            "family strength micros must be an integer in [0, 1000000]"
        )
    whole, fraction = divmod(value_micros, 1_000_000)
    return f"{whole}.{fraction:06d}"
