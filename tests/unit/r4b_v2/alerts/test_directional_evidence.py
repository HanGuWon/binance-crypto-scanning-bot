from __future__ import annotations

import pytest

from signalbot.r4b_v2.alerts.directional_evidence import (
    DirectionalEvidenceAlertContractErrorV2,
    render_directional_evidence_alert_v2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.directional_evidence import (
    DirectionalEvidencePanelDecisionV2,
    DirectionalStateClassV2,
    evaluate_directional_evidence_panel_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import EvidenceReadinessV2
from signalbot.r4b_v2.strategy.evidence_score import (
    assemble_evidence_score_input_v2,
)

from ..strategy.evidence_producer_testkit import (
    build_test_ownership_ledger_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2


def _decision(
    directions: tuple[int, ...] = (1, 1, 0, 0, 0, 1),
    *,
    strengths: tuple[int, ...] = (900_000, 600_000, 0, 0, 0, 300_000),
    readiness: tuple[EvidenceReadinessV2, ...] | None = None,
) -> DirectionalEvidencePanelDecisionV2:
    ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=directions,
        strengths=strengths,
        readiness=readiness,
    )
    return evaluate_directional_evidence_panel_v2(
        assemble_evidence_score_input_v2(ledger)
    )


def test_ready_annotation_reports_three_family_agreement_without_score_100() -> None:
    annotation = render_directional_evidence_alert_v2(_decision())

    assert annotation.headline.endswith("BTCUSDT | BROAD_BULLISH_STATE")
    assert annotation.agreement_text == (
        "Directional agreement: +0.600000 (uncalibrated descriptive index)"
    )
    assert annotation.family_count_text == "bullish 3 | bearish 0 | neutral 0"
    assert len(annotation.directional_evidence_lines) == 3
    assert "CROSS_SECTIONAL_CONTEXT_EX_TARGET" in annotation.directional_evidence_lines[2]
    assert "/100" not in annotation.agreement_text
    assert "%" not in annotation.agreement_text
    assert not annotation.standalone_alert
    assert not annotation.may_suppress_primary_alert
    assert not annotation.may_change_primary_decision


def test_context_source_signs_never_render_as_directional_votes() -> None:
    annotation = render_directional_evidence_alert_v2(
        _decision(
            directions=(1, 1, -1, 1, -1, 1),
            strengths=(900_000, 600_000, 1_000_000, 1_000_000, 1_000_000, 300_000),
        )
    )

    assert annotation.family_count_text == "bullish 3 | bearish 0 | neutral 0"
    assert len(annotation.context_lines) == 3
    assert all("direction excluded" in value for value in annotation.context_lines)
    assert all("BULLISH" not in value for value in annotation.context_lines)
    assert all("BEARISH" not in value for value in annotation.context_lines)


def test_withheld_annotation_exposes_no_partial_agreement() -> None:
    readiness = (
        EvidenceReadinessV2.FEATURE_NOT_READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
    )
    annotation = render_directional_evidence_alert_v2(
        _decision(
            directions=(0, 1, 0, 0, 0, 1),
            strengths=(0, 600_000, 0, 0, 0, 300_000),
            readiness=readiness,
        )
    )

    assert annotation.source_decision.state_class is DirectionalStateClassV2.WITHHELD
    assert annotation.agreement_text == "Directional agreement: WITHHELD"
    assert annotation.family_count_text == "required directional family unavailable"


def test_annotation_binds_identity_times_reasons_and_fixed_safety_language() -> None:
    decision = _decision()
    annotation = render_directional_evidence_alert_v2(decision)
    combined = "\n".join(
        (
            annotation.headline,
            annotation.agreement_text,
            annotation.family_count_text,
            *annotation.directional_evidence_lines,
            *annotation.context_lines,
            annotation.book_pressure_text,
            annotation.source_authority_text,
            annotation.disclaimer,
            annotation.invalidation,
            annotation.rule_version,
            *annotation.reason_lines,
        )
    )

    assert annotation.event_id == decision.event_id
    assert annotation.source_payload_sha256 == decision.payload_sha256
    assert "UTC " in annotation.time_text
    assert "KST " in annotation.time_text
    assert "not a probability" in annotation.disclaimer
    assert "order instruction" in annotation.disclaimer
    assert "expected return" in annotation.disclaimer
    assert annotation.source_authority_text.endswith(
        "LEGACY_OBSERVATIONS_M0_M1_M2_UNBOUND"
    )
    assert "HIGH PROBABILITY" not in combined.upper()
    assert "EXPECTED PROFIT" not in combined.upper()
    assert " BUY " not in f" {combined.upper()} "
    assert " SELL " not in f" {combined.upper()} "


def test_primary_relationship_stays_unavailable_without_a_sealed_binding() -> None:
    annotation = render_directional_evidence_alert_v2(_decision())

    assert all(
        "relationship=PRIMARY_BINDING_UNAVAILABLE" in value
        for value in (*annotation.directional_evidence_lines, *annotation.context_lines)
    )


def test_renderer_rejects_wrong_types() -> None:
    with pytest.raises(DirectionalEvidenceAlertContractErrorV2, match="decision must"):
        render_directional_evidence_alert_v2(object())  # type: ignore[arg-type]
