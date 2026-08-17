from __future__ import annotations

from dataclasses import replace

import pytest

from signalbot.r4b_v2.alerts.evidence import (
    render_evidence_alert_annotation_v2,
)
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.evidence_score import (
    EvidenceReadinessV2,
    EvidenceScoreInputV2,
    evaluate_evidence_score_v2,
)

from ..strategy.evidence_producer_testkit import build_test_observations_v2

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2


def _decision(*, ready: bool = True):
    observations = build_test_observations_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=D_MS,
        directions=(1, 1, 1, 1, 0, 0) if ready else (0, 0, 0, 0, 0, 0),
        readiness=(
            (EvidenceReadinessV2.READY,) * 6
            if ready
            else (
                EvidenceReadinessV2.FEATURE_NOT_READY,
                *(EvidenceReadinessV2.READY for _ in range(5)),
            )
        ),
    )
    return evaluate_evidence_score_v2(
        EvidenceScoreInputV2(
            attempt_id="attempt-1",
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256="a" * 64,
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=D_MS,
            closed_bar=True,
            causal_inputs_complete=True,
            observations=observations,
        )
    )


def test_ready_annotation_displays_score_counts_utc_kst_and_nonprobability_scope() -> None:
    annotation = render_evidence_alert_annotation_v2(_decision())

    assert annotation.headline.startswith("Evidence Score (not a probability)")
    assert annotation.score_text == "agreement=+66.6667/100"
    assert annotation.family_count_text == "bullish=4 | bearish=0 | neutral=2"
    assert len(annotation.family_evidence_lines) == 6
    assert "UTC " in annotation.time_text and "KST " in annotation.time_text
    assert "not a probability" in annotation.disclaimer
    assert "Primary A/B/C decision is unchanged" in annotation.disclaimer
    assert not annotation.standalone_alert
    assert not annotation.may_suppress_primary_alert
    assert not annotation.may_change_primary_decision


def test_public_annotation_model_rejects_probability_claim_injection() -> None:
    annotation = render_evidence_alert_annotation_v2(_decision())

    with pytest.raises(ValueError, match="init=False"):
        replace(annotation, headline="99% probability of a rally")
    with pytest.raises(ValueError, match="init=False"):
        replace(
            annotation,
            family_evidence_lines=(
                "PRICE_STRUCTURE_MOMENTUM: 99% probability",
                *annotation.family_evidence_lines[1:],
            ),
        )
    with pytest.raises(ValueError, match="init=False"):
        replace(annotation, time_text="99% probability")


def test_nonready_annotation_withholds_score_and_directional_counts() -> None:
    annotation = render_evidence_alert_annotation_v2(_decision(ready=False))

    assert annotation.score_text == "UNAVAILABLE"
    assert annotation.status_text == EvidenceReadinessV2.FEATURE_NOT_READY.value
    assert annotation.family_count_text.startswith("WITHHELD")
    assert all("strength=0.000000" in line for line in annotation.family_evidence_lines)
