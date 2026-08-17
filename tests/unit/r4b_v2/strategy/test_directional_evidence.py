from __future__ import annotations

import json
from dataclasses import replace
from itertools import product

import pytest

from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.directional_evidence import (
    DIRECTIONAL_EVIDENCE_ROLE_V2,
    DirectionalEvidenceContractErrorV2,
    DirectionalEvidencePanelRegistryV2,
    DirectionalEvidenceRegistryDispositionV2,
    DirectionalStateClassV2,
    EvidencePanelRoleV2,
    PrimaryBindingStatusV2,
    PrimaryEvidenceRelationshipV2,
    canonical_directional_evidence_panel_v2,
    evaluate_directional_evidence_panel_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import (
    EVIDENCE_STRENGTH_SCALE_V2,
    EvidenceInformationFamilyV2,
    EvidenceReadinessV2,
)
from signalbot.r4b_v2.strategy.evidence_score import (
    EvidenceScoreInputV2,
    assemble_evidence_score_input_v2,
)

from .evidence_producer_testkit import build_test_ownership_ledger_v2

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2


def _input(
    directions: tuple[int, ...] = (0, 0, 0, 0, 0, 0),
    *,
    strengths: tuple[int, ...] | None = None,
    readiness: tuple[EvidenceReadinessV2, ...] | None = None,
    closed_bar: bool = True,
    causal_inputs_complete: bool = True,
    latest_source_event_ms: int | None = None,
    latest_source_receipt_ms: int | None = None,
    attempt_id: str = "attempt-1",
) -> EvidenceScoreInputV2:
    ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=directions,
        strengths=strengths,
        readiness=readiness,
        closed_bar=closed_bar,
        causal_inputs_complete=causal_inputs_complete,
        latest_source_event_ms=latest_source_event_ms,
        latest_source_receipt_ms=latest_source_receipt_ms,
        attempt_id=attempt_id,
    )
    return assemble_evidence_score_input_v2(ledger)


def _six_family_directions(
    directional_signs: tuple[int, int, int],
    *,
    context_signs: tuple[int, int, int] = (0, 0, 0),
) -> tuple[int, ...]:
    price, participation, cross = directional_signs
    volatility, derivatives, liquidity = context_signs
    return (
        price,
        participation,
        volatility,
        derivatives,
        liquidity,
        cross,
    )


def _strengths_for(directions: tuple[int, ...], magnitude: int = 600_000) -> tuple[int, ...]:
    return tuple(magnitude if direction else 0 for direction in directions)


def _expected_class(signs: tuple[int, int, int]) -> DirectionalStateClassV2:
    bullish = sum(value == 1 for value in signs)
    bearish = sum(value == -1 for value in signs)
    if bullish == 3 and bearish == 0:
        return DirectionalStateClassV2.BROAD_BULLISH_STATE
    if bullish >= 2 and bearish == 0:
        return DirectionalStateClassV2.BULLISH_STATE_TILT
    if bearish == 3 and bullish == 0:
        return DirectionalStateClassV2.BROAD_BEARISH_STATE
    if bearish >= 2 and bullish == 0:
        return DirectionalStateClassV2.BEARISH_STATE_TILT
    return DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE


@pytest.mark.parametrize("signs", tuple(product((-1, 0, 1), repeat=3)))
def test_all_27_direction_combinations_have_the_frozen_class_order(
    signs: tuple[int, int, int],
) -> None:
    directions = _six_family_directions(signs)
    decision = evaluate_directional_evidence_panel_v2(
        _input(directions, strengths=_strengths_for(directions))
    )

    assert decision.ready
    assert decision.state_class is _expected_class(signs)
    assert decision.directional_denominator == 3
    assert decision.bullish_family_count == sum(value == 1 for value in signs)
    assert decision.bearish_family_count == sum(value == -1 for value in signs)
    assert decision.neutral_family_count == sum(value == 0 for value in signs)


def test_exact_signed_magnitude_and_context_directions_are_isolated() -> None:
    directions = _six_family_directions(
        (1, 1, 1),
        context_signs=(-1, 1, -1),
    )
    decision = evaluate_directional_evidence_panel_v2(
        _input(
            directions,
            strengths=(900_000, 600_000, 1_000_000, 1_000_000, 1_000_000, 300_000),
        )
    )

    assert decision.state_class is DirectionalStateClassV2.BROAD_BULLISH_STATE
    assert decision.directional_numerator_micros == 1_800_000
    assert decision.directional_agreement_micros == 600_000
    assert decision.directional_denominator == 3
    assert "CONTEXT_FAMILIES_EXCLUDED_FROM_DIRECTIONAL_NUMERATOR" in decision.reasons


def test_context_changes_alone_cannot_change_directional_fields() -> None:
    directional = (1, 0, -1)
    first_directions = _six_family_directions(
        directional,
        context_signs=(-1, -1, -1),
    )
    second_directions = _six_family_directions(
        directional,
        context_signs=(1, 1, 1),
    )
    first = evaluate_directional_evidence_panel_v2(
        _input(first_directions, strengths=_strengths_for(first_directions))
    )
    second = evaluate_directional_evidence_panel_v2(
        _input(second_directions, strengths=_strengths_for(second_directions))
    )

    assert first.state_class is second.state_class
    assert first.directional_numerator_micros == second.directional_numerator_micros
    assert first.directional_agreement_micros == second.directional_agreement_micros
    assert first.bullish_family_count == second.bullish_family_count
    assert first.bearish_family_count == second.bearish_family_count
    assert first.neutral_family_count == second.neutral_family_count


def test_two_supporting_families_with_one_weak_opponent_remain_mixed() -> None:
    directions = _six_family_directions((1, 1, -1))
    decision = evaluate_directional_evidence_panel_v2(
        _input(
            directions,
            strengths=(900_000, 900_000, 0, 0, 0, 1),
        )
    )

    assert decision.directional_agreement_micros == 600_000
    assert decision.state_class is DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE


def test_small_signed_values_round_symmetrically_to_nearest_integer() -> None:
    bullish_directions = _six_family_directions((1, 0, 0))
    bearish_directions = _six_family_directions((-1, 0, 0))
    bullish = evaluate_directional_evidence_panel_v2(
        _input(
            bullish_directions,
            strengths=(2, 0, 0, 0, 0, 0),
        )
    )
    bearish = evaluate_directional_evidence_panel_v2(
        _input(
            bearish_directions,
            strengths=(2, 0, 0, 0, 0, 0),
        )
    )

    assert bullish.directional_agreement_micros == 1
    assert bearish.directional_agreement_micros == -1


def test_any_nonready_directional_family_withholds_without_fabricating_zero() -> None:
    readiness = (
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.FEATURE_NOT_READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
    )
    decision = evaluate_directional_evidence_panel_v2(
        _input(readiness=readiness)
    )

    assert not decision.ready
    assert decision.status is EvidenceReadinessV2.FEATURE_NOT_READY
    assert decision.state_class is DirectionalStateClassV2.WITHHELD
    assert decision.directional_numerator_micros is None
    assert decision.directional_denominator is None
    assert decision.directional_agreement_micros is None
    assert decision.bullish_family_count is None


def test_nonready_context_is_reported_but_does_not_withhold_directional_panel() -> None:
    directions = _six_family_directions((1, 1, 1))
    readiness = (
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.FEATURE_NOT_READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.INCONCLUSIVE_DATA,
        EvidenceReadinessV2.READY,
    )
    strengths = tuple(
        EVIDENCE_STRENGTH_SCALE_V2
        if direction and status is EvidenceReadinessV2.READY
        else 0
        for direction, status in zip(directions, readiness, strict=True)
    )
    decision = evaluate_directional_evidence_panel_v2(
        _input(directions, strengths=strengths, readiness=readiness)
    )

    assert decision.ready
    assert decision.state_class is DirectionalStateClassV2.BROAD_BULLISH_STATE
    assert decision.context_unavailable_families == (
        EvidenceInformationFamilyV2.VOLATILITY_REGIME,
        EvidenceInformationFamilyV2.LIQUIDITY_EXECUTION,
    )
    assert (
        "CONTEXT_WITHHELD:VOLATILITY_REGIME:FEATURE_NOT_READY"
        in decision.reasons
    )


@pytest.mark.parametrize(
    ("closed_bar", "causal_inputs_complete", "expected_status"),
    (
        (False, True, EvidenceReadinessV2.DATA_INVALID),
        (True, False, EvidenceReadinessV2.INCONCLUSIVE_DATA),
    ),
)
def test_directional_causal_failures_fail_closed(
    closed_bar: bool,
    causal_inputs_complete: bool,
    expected_status: EvidenceReadinessV2,
) -> None:
    decision = evaluate_directional_evidence_panel_v2(
        _input(
            closed_bar=closed_bar,
            causal_inputs_complete=causal_inputs_complete,
        )
    )

    assert decision.status is expected_status
    assert decision.state_class is DirectionalStateClassV2.WITHHELD


@pytest.mark.parametrize(
    ("event_ms", "receipt_ms"),
    (
        (DECISION_CUTOFF_MS, DECISION_CUTOFF_MS - 1),
        (BAR_CLOSE_MS, DECISION_CUTOFF_MS + 1),
    ),
)
def test_invalid_event_or_receipt_order_fails_closed(
    event_ms: int,
    receipt_ms: int,
) -> None:
    decision = evaluate_directional_evidence_panel_v2(
        _input(
            latest_source_event_ms=event_ms,
            latest_source_receipt_ms=receipt_ms,
        )
    )

    assert decision.status is EvidenceReadinessV2.DATA_INVALID
    assert decision.state_class is DirectionalStateClassV2.WITHHELD


def test_exchange_event_before_close_is_valid_when_closed_data_through_is_bound() -> None:
    decision = evaluate_directional_evidence_panel_v2(
        _input(
            latest_source_event_ms=BAR_CLOSE_MS - 1,
            latest_source_receipt_ms=DECISION_CUTOFF_MS,
        )
    )
    document = json.loads(canonical_directional_evidence_panel_v2(decision))

    assert decision.ready
    assert all(value["data_through_ms"] is None for value in document["observations"])
    assert all(
        value["data_through_status"] == "UNBOUND_M2"
        for value in document["observations"]
    )
    assert all(
        value["assumed_closed_bar_through_ms"] == BAR_CLOSE_MS
        for value in document["observations"]
    )
    assert all(
        value["exchange_event_ms"] == BAR_CLOSE_MS - 1
        for value in document["observations"]
    )
    assert all(
        value["local_receipt_ms"] == DECISION_CUTOFF_MS
        for value in document["observations"]
    )


def test_incomplete_causal_inputs_never_fabricate_certified_data_through() -> None:
    decision = evaluate_directional_evidence_panel_v2(
        _input(causal_inputs_complete=False)
    )
    document = json.loads(canonical_directional_evidence_panel_v2(decision))

    assert decision.status is EvidenceReadinessV2.INCONCLUSIVE_DATA
    assert all(value["data_through_ms"] is None for value in document["observations"])
    assert all(
        value["data_through_status"] == "UNBOUND_M2"
        for value in document["observations"]
    )


def test_input_permutation_produces_identical_decision_and_payload() -> None:
    directions = _six_family_directions((1, 0, -1))
    item = _input(directions, strengths=_strengths_for(directions))
    reversed_item = replace(item, observations=tuple(reversed(item.observations)))

    first = evaluate_directional_evidence_panel_v2(item)
    second = evaluate_directional_evidence_panel_v2(reversed_item)

    assert first == second
    assert first.event_id == second.event_id
    assert first.payload_sha256 == second.payload_sha256
    assert canonical_directional_evidence_panel_v2(first) == (
        canonical_directional_evidence_panel_v2(second)
    )


def test_canonical_payload_marks_context_direction_as_ignored() -> None:
    directions = _six_family_directions(
        (1, 1, 1),
        context_signs=(1, -1, 1),
    )
    decision = evaluate_directional_evidence_panel_v2(
        _input(directions, strengths=_strengths_for(directions))
    )
    document = json.loads(canonical_directional_evidence_panel_v2(decision))
    observations = {value["family"]: value for value in document["observations"]}

    assert document["directional_denominator"] == 3
    assert document["promoting"] is False
    assert document["probability_calibrated"] is False
    assert document["role"] == DIRECTIONAL_EVIDENCE_ROLE_V2
    assert document["book_pressure_status"] == "NOT_CONNECTED_SHADOW_CANDIDATE"
    assert document["source_authority_status"] == (
        "LEGACY_OBSERVATIONS_M0_M1_M2_UNBOUND"
    )
    assert document["primary_binding_status"] == (
        PrimaryBindingStatusV2.UNAVAILABLE.value
    )
    assert observations["PRICE_STRUCTURE_MOMENTUM"]["panel_role"] == (
        EvidencePanelRoleV2.DIRECTIONAL_STATE.value
    )
    assert observations["VOLATILITY_REGIME"]["panel_role"] == (
        EvidencePanelRoleV2.CONTEXT_ONLY_DIRECTION_IGNORED.value
    )
    assert observations["VOLATILITY_REGIME"]["direction_in_numerator"] is False
    assert observations["VOLATILITY_REGIME"]["source_direction_ignored"] is True
    assert "direction" not in observations["VOLATILITY_REGIME"]
    assert "strength_micros" not in observations["VOLATILITY_REGIME"]
    assert observations["VOLATILITY_REGIME"]["context_intensity_status"] == (
        "NOT_AVAILABLE_IN_LEGACY_SCHEMA"
    )


@pytest.mark.parametrize("family", tuple(EvidenceInformationFamilyV2))
def test_primary_relationship_is_unavailable_until_a_sealed_decision_is_bound(
    family: EvidenceInformationFamilyV2,
) -> None:
    decision = evaluate_directional_evidence_panel_v2(_input())

    assert decision.primary_binding_status is PrimaryBindingStatusV2.UNAVAILABLE
    assert decision.primary_relationship(family) is (
        PrimaryEvidenceRelationshipV2.PRIMARY_BINDING_UNAVAILABLE
    )


def test_factory_boundary_and_input_types_fail_loudly() -> None:
    decision = evaluate_directional_evidence_panel_v2(_input())

    with pytest.raises(
        DirectionalEvidenceContractErrorV2,
        match="frozen evaluator",
    ):
        replace(decision, state_class=DirectionalStateClassV2.BROAD_BULLISH_STATE)
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="item must"):
        evaluate_directional_evidence_panel_v2(object())  # type: ignore[arg-type]
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="family must"):
        decision.primary_relationship("PRICE")  # type: ignore[arg-type]


def test_bounded_registry_is_idempotent_and_rejects_same_id_conflicts() -> None:
    first = evaluate_directional_evidence_panel_v2(_input())
    changed_directions = _six_family_directions((1, 0, 0))
    changed = evaluate_directional_evidence_panel_v2(
        _input(
            changed_directions,
            strengths=_strengths_for(changed_directions),
        )
    )
    registry = DirectionalEvidencePanelRegistryV2(maximum_events=2)

    assert registry.register(first) is DirectionalEvidenceRegistryDispositionV2.NEW
    assert registry.register(first) is (
        DirectionalEvidenceRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    assert first.event_id == changed.event_id
    assert first.payload_sha256 != changed.payload_sha256
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="collides"):
        registry.register(changed)


def test_bounded_registry_rejects_overflow_and_wrong_types() -> None:
    first = evaluate_directional_evidence_panel_v2(_input())
    second = evaluate_directional_evidence_panel_v2(_input(attempt_id="attempt-2"))
    registry = DirectionalEvidencePanelRegistryV2(maximum_events=1)

    registry.register(first)
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="capacity"):
        registry.register(second)
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="registry accepts"):
        registry.register(object())  # type: ignore[arg-type]
    assert registry.event_count == 1


@pytest.mark.parametrize("maximum_events", (0, -1, True, 1.0))
def test_registry_capacity_contract_is_strict(maximum_events: object) -> None:
    with pytest.raises(DirectionalEvidenceContractErrorV2, match="positive integer"):
        DirectionalEvidencePanelRegistryV2(
            maximum_events=maximum_events,  # type: ignore[arg-type]
        )
