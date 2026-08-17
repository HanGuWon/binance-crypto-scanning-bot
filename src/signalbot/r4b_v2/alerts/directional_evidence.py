from __future__ import annotations

from dataclasses import dataclass, field

from signalbot.r4b_v2.alerts.evidence import format_evidence_times_v2
from signalbot.r4b_v2.strategy.directional_evidence import (
    DirectionalEvidencePanelDecisionV2,
)
from signalbot.r4b_v2.strategy.evidence_producer import (
    EvidenceInformationFamilyV2,
)

_HEADLINE_PREFIX = "Directional Evidence (shadow) | "
_FIXED_DISCLAIMER = (
    "Uncalibrated descriptive agreement only; not a probability, expected "
    "return, profitability claim, or order instruction. Primary A/B/C decision "
    "is unchanged."
)
_DISPLAY_FAMILY = {
    EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM: (
        "PRICE_STRUCTURE_MOMENTUM"
    ),
    EvidenceInformationFamilyV2.PARTICIPATION_FLOW: "PARTICIPATION_FLOW",
    EvidenceInformationFamilyV2.CROSS_SECTIONAL_CONTEXT: (
        "CROSS_SECTIONAL_CONTEXT_EX_TARGET"
    ),
}


class DirectionalEvidenceAlertContractErrorV2(ValueError):
    """Raised when a shadow directional annotation cannot be rendered safely."""


@dataclass(frozen=True, slots=True)
class DirectionalEvidenceAlertAnnotationV2:
    """Decision-bound display fields with no caller-injected trading language."""

    source_decision: DirectionalEvidencePanelDecisionV2 = field(repr=False)
    event_id: str = field(init=False)
    source_payload_sha256: str = field(init=False)
    headline: str = field(init=False)
    agreement_text: str = field(init=False)
    family_count_text: str = field(init=False)
    directional_evidence_lines: tuple[str, ...] = field(init=False)
    context_lines: tuple[str, ...] = field(init=False)
    book_pressure_text: str = field(init=False)
    status_text: str = field(init=False)
    time_text: str = field(init=False)
    reason_lines: tuple[str, ...] = field(init=False)
    source_authority_text: str = field(init=False)
    disclaimer: str = field(init=False, default=_FIXED_DISCLAIMER)
    invalidation: str = field(init=False)
    rule_version: str = field(init=False)
    role: str = field(init=False)
    standalone_alert: bool = field(init=False, default=False)
    may_suppress_primary_alert: bool = field(init=False, default=False)
    may_change_primary_decision: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        decision = self.source_decision
        if not isinstance(decision, DirectionalEvidencePanelDecisionV2):
            raise DirectionalEvidenceAlertContractErrorV2(
                "source_decision must be a DirectionalEvidencePanelDecisionV2"
            )
        if decision.ready:
            assert decision.directional_agreement_micros is not None
            assert decision.bullish_family_count is not None
            assert decision.bearish_family_count is not None
            assert decision.neutral_family_count is not None
            agreement_text = (
                "Directional agreement: "
                f"{_signed_micros(decision.directional_agreement_micros)} "
                "(uncalibrated descriptive index)"
            )
            family_count_text = (
                f"bullish {decision.bullish_family_count} | "
                f"bearish {decision.bearish_family_count} | "
                f"neutral {decision.neutral_family_count}"
            )
        else:
            agreement_text = "Directional agreement: WITHHELD"
            family_count_text = "required directional family unavailable"

        directional_lines = tuple(
            (
                f"{_DISPLAY_FAMILY[value.family]}: "
                f"{_direction_label(value.direction)} "
                f"magnitude={_unsigned_micros(value.strength_micros)} | "
                f"status={decision.effective_readiness(value.family).value} | "
                f"relationship={decision.primary_relationship(value.family).value}"
            )
            for value in decision.directional_observations
        )
        context_lines = tuple(
            (
                f"{value.family.value}: "
                f"status={decision.effective_readiness(value.family).value} | "
                "direction excluded | "
                f"relationship={decision.primary_relationship(value.family).value}"
            )
            for value in decision.context_observations
        )
        object.__setattr__(self, "event_id", decision.event_id)
        object.__setattr__(self, "source_payload_sha256", decision.payload_sha256)
        object.__setattr__(
            self,
            "headline",
            f"{_HEADLINE_PREFIX}{decision.symbol} | {decision.state_class.value}",
        )
        object.__setattr__(self, "agreement_text", agreement_text)
        object.__setattr__(self, "family_count_text", family_count_text)
        object.__setattr__(self, "directional_evidence_lines", directional_lines)
        object.__setattr__(self, "context_lines", context_lines)
        object.__setattr__(
            self,
            "book_pressure_text",
            f"BOOK_PRESSURE: {decision.book_pressure_status}",
        )
        object.__setattr__(self, "status_text", decision.status.value)
        object.__setattr__(
            self,
            "time_text",
            format_evidence_times_v2(decision.bar_close_ms),
        )
        object.__setattr__(self, "reason_lines", decision.reasons)
        object.__setattr__(
            self,
            "source_authority_text",
            f"Source authority: {decision.source_authority_status}",
        )
        object.__setattr__(self, "invalidation", decision.invalidation)
        object.__setattr__(self, "rule_version", decision.rule_version)
        object.__setattr__(self, "role", decision.role)


def render_directional_evidence_alert_v2(
    decision: DirectionalEvidencePanelDecisionV2,
) -> DirectionalEvidenceAlertAnnotationV2:
    """Render deterministic shadow fields without probability or order semantics."""

    if not isinstance(decision, DirectionalEvidencePanelDecisionV2):
        raise DirectionalEvidenceAlertContractErrorV2(
            "decision must be a DirectionalEvidencePanelDecisionV2"
        )
    return DirectionalEvidenceAlertAnnotationV2(source_decision=decision)


def _direction_label(direction: int) -> str:
    if direction == 1:
        return "BULLISH"
    if direction == -1:
        return "BEARISH"
    if direction == 0:
        return "NEUTRAL"
    raise DirectionalEvidenceAlertContractErrorV2(
        "unsupported directional family sign"
    )


def _signed_micros(value: int) -> str:
    if type(value) is not int or not -1_000_000 <= value <= 1_000_000:
        raise DirectionalEvidenceAlertContractErrorV2(
            "directional agreement must be in [-1000000, 1000000]"
        )
    sign = "+" if value >= 0 else "-"
    whole, fraction = divmod(abs(value), 1_000_000)
    return f"{sign}{whole}.{fraction:06d}"


def _unsigned_micros(value: int) -> str:
    if type(value) is not int or not 0 <= value <= 1_000_000:
        raise DirectionalEvidenceAlertContractErrorV2(
            "directional magnitude must be in [0, 1000000]"
        )
    whole, fraction = divmod(value, 1_000_000)
    return f"{whole}.{fraction:06d}"
