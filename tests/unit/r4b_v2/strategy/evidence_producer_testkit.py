from __future__ import annotations

import hashlib

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.strategy.evidence_producer import (
    EVIDENCE_STRENGTH_SCALE_V2,
    DependencyOwnershipLedgerV2,
    EvidenceDependencyClaimV2,
    EvidenceDependencyClassV2,
    EvidenceFamilyObservationV2,
    EvidenceInformationFamilyV2,
    EvidenceReadinessV2,
    ProducerEvidenceEnvelopeV2,
    _seal_producer_evidence_envelope_v2,
)

_DEPENDENCY_BY_FAMILY = {
    EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM: (
        EvidenceDependencyClassV2.TARGET_CLOSE_PATH
    ),
    EvidenceInformationFamilyV2.PARTICIPATION_FLOW: (
        EvidenceDependencyClassV2.NORMAL_AGGTRADE_FLOW
    ),
    EvidenceInformationFamilyV2.VOLATILITY_REGIME: (
        EvidenceDependencyClassV2.TARGET_HIGH_LOW_RANGE
    ),
    EvidenceInformationFamilyV2.DERIVATIVES_POSITIONING: (
        EvidenceDependencyClassV2.MARK_OI_POSITIONING
    ),
    EvidenceInformationFamilyV2.LIQUIDITY_EXECUTION: (
        EvidenceDependencyClassV2.STANDARD_DIFF_DEPTH_SYMMETRIC
    ),
    EvidenceInformationFamilyV2.CROSS_SECTIONAL_CONTEXT: (
        EvidenceDependencyClassV2.TARGET_EXCLUDED_CROSS_SECTION
    ),
}


def digest_test_value_v2(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def build_test_producer_envelope_v2(
    *,
    family: EvidenceInformationFamilyV2,
    direction: int = 0,
    strength_micros: int | None = None,
    readiness: EvidenceReadinessV2 = EvidenceReadinessV2.READY,
    attempt_id: str = "attempt-1",
    symbol: str = "BTCUSDT",
    venue: VenueV2 = VenueV2.USDM_FUTURES,
    promoting_plan_sha256: str = "a" * 64,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    latest_source_event_ms: int | None = None,
    latest_source_receipt_ms: int | None = None,
    closed_bar: bool = True,
    causal_inputs_complete: bool = True,
    economic_slice_sha256: str | None = None,
    source_lineage_root_sha256: str | None = None,
    producer_evidence_sha256: str | None = None,
) -> ProducerEvidenceEnvelopeV2:
    if strength_micros is None:
        strength_micros = EVIDENCE_STRENGTH_SCALE_V2 if direction else 0
    family_label = family.value
    lineage_root = source_lineage_root_sha256 or digest_test_value_v2(
        f"lineage:{attempt_id}:{symbol}:{bar_open_ms}:{family_label}"
    )
    slice_root = economic_slice_sha256 or digest_test_value_v2(
        f"economic-slice:{attempt_id}:{symbol}:{bar_open_ms}:{family_label}"
    )
    evidence_root = producer_evidence_sha256 or digest_test_value_v2(
        f"producer-evidence:{attempt_id}:{symbol}:{bar_open_ms}:{family_label}"
    )
    claim = EvidenceDependencyClaimV2(
        dependency_class=_DEPENDENCY_BY_FAMILY[family],
        economic_slice_sha256=slice_root,
        source_lineage_root_sha256=lineage_root,
    )
    return _seal_producer_evidence_envelope_v2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        family=family,
        readiness=readiness,
        direction=direction,
        strength_micros=strength_micros,
        producer_version_id=f"TEST_{family_label}_V2",
        source_lineage_root_sha256=lineage_root,
        feature_slice_root_sha256=digest_test_value_v2(
            f"feature-slice:{attempt_id}:{symbol}:{bar_open_ms}:{family_label}"
        ),
        producer_evidence_sha256=evidence_root,
        dependency_claims=(claim,),
        latest_source_event_ms=(
            bar_close_ms
            if latest_source_event_ms is None
            else latest_source_event_ms
        ),
        latest_source_receipt_ms=(
            decision_cutoff_ms
            if latest_source_receipt_ms is None
            else latest_source_receipt_ms
        ),
        closed_bar=closed_bar,
        causal_inputs_complete=causal_inputs_complete,
        reasons=(f"{family_label}_TEST_EVIDENCE",),
    )


def build_test_ownership_ledger_v2(
    *,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    directions: tuple[int, ...] = (0, 0, 0, 0, 0, 0),
    strengths: tuple[int, ...] | None = None,
    readiness: tuple[EvidenceReadinessV2, ...] | None = None,
    attempt_id: str = "attempt-1",
    symbol: str = "BTCUSDT",
    venue: VenueV2 = VenueV2.USDM_FUTURES,
    promoting_plan_sha256: str = "a" * 64,
    latest_source_event_ms: int | None = None,
    latest_source_receipt_ms: int | None = None,
    closed_bar: bool = True,
    causal_inputs_complete: bool = True,
    registration_order: tuple[EvidenceInformationFamilyV2, ...] | None = None,
    economic_slice_overrides: dict[EvidenceInformationFamilyV2, str] | None = None,
    lineage_overrides: dict[EvidenceInformationFamilyV2, str] | None = None,
    producer_evidence_overrides: dict[EvidenceInformationFamilyV2, str]
    | None = None,
) -> DependencyOwnershipLedgerV2:
    families = tuple(EvidenceInformationFamilyV2)
    if strengths is None:
        strengths = tuple(
            EVIDENCE_STRENGTH_SCALE_V2 if direction else 0
            for direction in directions
        )
    if readiness is None:
        readiness = (EvidenceReadinessV2.READY,) * len(families)
    if not (
        len(directions) == len(strengths) == len(readiness) == len(families)
    ):
        raise ValueError("test producer vectors must contain exactly six values")
    envelopes = {
        family: build_test_producer_envelope_v2(
            family=family,
            direction=direction,
            strength_micros=strength,
            readiness=status,
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
            latest_source_event_ms=latest_source_event_ms,
            latest_source_receipt_ms=latest_source_receipt_ms,
            closed_bar=closed_bar,
            causal_inputs_complete=causal_inputs_complete,
            economic_slice_sha256=(
                None
                if economic_slice_overrides is None
                else economic_slice_overrides.get(family)
            ),
            source_lineage_root_sha256=(
                None
                if lineage_overrides is None
                else lineage_overrides.get(family)
            ),
            producer_evidence_sha256=(
                None
                if producer_evidence_overrides is None
                else producer_evidence_overrides.get(family)
            ),
        )
        for family, status, direction, strength in zip(
            families,
            readiness,
            directions,
            strengths,
            strict=True,
        )
    }
    ledger = DependencyOwnershipLedgerV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    for family in registration_order or families:
        ledger.register(envelopes[family])
    return ledger


def build_test_observations_v2(
    **kwargs: object,
) -> tuple[EvidenceFamilyObservationV2, ...]:
    ledger = build_test_ownership_ledger_v2(**kwargs)  # type: ignore[arg-type]
    return ledger.finalize_observations_v2()
