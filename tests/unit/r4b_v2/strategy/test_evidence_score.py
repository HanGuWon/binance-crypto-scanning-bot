from __future__ import annotations

import json
from dataclasses import replace

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.lifecycle import AttemptAlphaRegistryV2
from signalbot.r4b_v2.strategy.evidence_producer import (
    DependencyOwnershipDispositionV2,
    DependencyOwnershipLedgerV2,
    EvidenceProducerContractErrorV2,
    ProducerEvidenceEnvelopeV2,
)
from signalbot.r4b_v2.strategy.evidence_score import (
    EVIDENCE_SCORE_ROLE_V2,
    EVIDENCE_STRENGTH_SCALE_V2,
    EvidenceAnnotationRegistryV2,
    EvidenceBiasV2,
    EvidenceFamilyObservationV2,
    EvidenceInformationFamilyV2,
    EvidenceReadinessV2,
    EvidenceRegistryDispositionV2,
    EvidenceScoreContractErrorV2,
    EvidenceScoreInputV2,
    assemble_evidence_score_input_v2,
    canonical_evidence_score_decision_v2,
    evaluate_evidence_score_v2,
)

from .evidence_producer_testkit import (
    build_test_observations_v2,
    build_test_ownership_ledger_v2,
    build_test_producer_envelope_v2,
    digest_test_value_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2
PLAN_SHA256 = "a" * 64
FAMILIES = tuple(EvidenceInformationFamilyV2)


def _observations(
    directions: tuple[int, ...] = (0, 0, 0, 0, 0, 0),
    strengths: tuple[int, ...] | None = None,
    *,
    readiness: tuple[EvidenceReadinessV2, ...] | None = None,
    event_ms: int = BAR_CLOSE_MS,
    receipt_ms: int = DECISION_CUTOFF_MS,
    attempt_id: str = "attempt-1",
    symbol: str = "BTCUSDT",
    closed_bar: bool = True,
    causal_inputs_complete: bool = True,
) -> tuple[EvidenceFamilyObservationV2, ...]:
    if strengths is None:
        strengths = tuple(
            EVIDENCE_STRENGTH_SCALE_V2 if direction else 0
            for direction in directions
        )
    if readiness is None:
        readiness = (EvidenceReadinessV2.READY,) * len(FAMILIES)
    return build_test_observations_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=directions,
        strengths=strengths,
        readiness=readiness,
        attempt_id=attempt_id,
        symbol=symbol,
        latest_source_event_ms=event_ms,
        latest_source_receipt_ms=receipt_ms,
        closed_bar=closed_bar,
        causal_inputs_complete=causal_inputs_complete,
    )


def _input(**overrides: object) -> EvidenceScoreInputV2:
    values: dict[str, object] = {
        "attempt_id": "attempt-1",
        "symbol": "BTCUSDT",
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PLAN_SHA256,
        "bar_open_ms": BAR_OPEN_MS,
        "bar_close_ms": BAR_CLOSE_MS,
        "decision_cutoff_ms": DECISION_CUTOFF_MS,
        "closed_bar": True,
        "causal_inputs_complete": True,
    }
    values.update(overrides)
    if "observations" not in overrides:
        values["observations"] = _observations(
            attempt_id=values["attempt_id"],  # type: ignore[arg-type]
            symbol=values["symbol"],  # type: ignore[arg-type]
            closed_bar=values["closed_bar"],  # type: ignore[arg-type]
            causal_inputs_complete=values[  # type: ignore[arg-type]
                "causal_inputs_complete"
            ],
        )
    return EvidenceScoreInputV2(**values)  # type: ignore[arg-type]


def _empty_ledger(
    *,
    attempt_id: str = "attempt-1",
    symbol: str = "BTCUSDT",
) -> DependencyOwnershipLedgerV2:
    return DependencyOwnershipLedgerV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
    )


def test_four_of_six_full_bullish_families_emit_strong_nonprobability_annotation() -> None:
    decision = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )

    assert decision.ready
    assert decision.bias is EvidenceBiasV2.BULLISH_STRONG
    assert decision.score_numerator_micros == 4_000_000
    assert decision.score_denominator == 6
    assert decision.evidence_score_micros == 666_667
    assert (
        decision.bullish_family_count,
        decision.bearish_family_count,
        decision.neutral_family_count,
    ) == (4, 0, 2)
    assert decision.role == EVIDENCE_SCORE_ROLE_V2
    assert not decision.promoting
    assert not decision.changes_primary_decision
    assert not decision.probability_calibrated
    assert not hasattr(decision, "probability")
    assert "EVIDENCE_SCORE_IS_NOT_A_PROBABILITY" in decision.reasons


def test_bearish_score_is_exactly_symmetric() -> None:
    bullish = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )
    bearish = evaluate_evidence_score_v2(
        _input(observations=_observations((-1, -1, -1, -1, 0, 0)))
    )

    assert bullish.score_numerator_micros is not None
    assert bullish.evidence_score_micros is not None
    assert bearish.bias is EvidenceBiasV2.BEARISH_STRONG
    assert bearish.score_numerator_micros == -bullish.score_numerator_micros
    assert bearish.evidence_score_micros == -bullish.evidence_score_micros
    assert bearish.bearish_family_count == bullish.bullish_family_count


def test_lean_and_strong_exact_boundaries_are_inclusive_and_one_micro_fails() -> None:
    lean_strengths = (666_667, 666_667, 666_666, 0, 0, 0)
    lean = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                (1, 1, 1, 0, 0, 0),
                lean_strengths,
            )
        )
    )
    below_lean = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                (1, 1, 1, 0, 0, 0),
                (666_667, 666_667, 666_665, 0, 0, 0),
            )
        )
    )
    strong = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )
    below_strong = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                (1, 1, 1, 1, 0, 0),
                (1_000_000, 1_000_000, 1_000_000, 999_999, 0, 0),
            )
        )
    )

    assert lean.bias is EvidenceBiasV2.BULLISH_LEAN
    assert below_lean.bias is EvidenceBiasV2.NEUTRAL
    assert strong.bias is EvidenceBiasV2.BULLISH_STRONG
    assert below_strong.bias is EvidenceBiasV2.BULLISH_LEAN


def test_an_additional_distinct_supporting_family_monotonically_increases_score() -> None:
    three = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 0, 0, 0)))
    )
    four = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )

    assert three.evidence_score_micros is not None
    assert four.evidence_score_micros is not None
    assert four.evidence_score_micros > three.evidence_score_micros
    assert four.bullish_family_count == three.bullish_family_count + 1  # type: ignore[operator]


def test_family_order_is_canonical_and_permutation_invariant() -> None:
    observations = _observations((1, 0, -1, 1, 0, 0))
    first = evaluate_evidence_score_v2(_input(observations=observations))
    second = evaluate_evidence_score_v2(
        _input(observations=tuple(reversed(observations)))
    )

    assert first == second
    assert first.event_id == second.event_id
    assert first.payload_sha256 == second.payload_sha256
    assert canonical_evidence_score_decision_v2(first) == (
        canonical_evidence_score_decision_v2(second)
    )


def test_ledger_registration_permutation_has_one_canonical_final_root() -> None:
    forward = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=(1, 0, -1, 1, 0, 0),
    )
    reverse = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=(1, 0, -1, 1, 0, 0),
        registration_order=tuple(reversed(FAMILIES)),
    )

    assert forward.finalize_observations_v2() == reverse.finalize_observations_v2()
    assert forward.replay_root_sha256 == reverse.replay_root_sha256
    assert forward.export_state_v2() == reverse.export_state_v2()


def test_factory_only_envelope_and_observation_reject_direct_or_replace_paths() -> None:
    envelope = build_test_producer_envelope_v2(
        family=FAMILIES[0],
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
    )
    observation = _observations()[0]

    with pytest.raises(EvidenceProducerContractErrorV2, match="producer factory"):
        ProducerEvidenceEnvelopeV2(
            attempt_id=envelope.attempt_id,
            symbol=envelope.symbol,
            venue=envelope.venue,
            promoting_plan_sha256=envelope.promoting_plan_sha256,
            bar_open_ms=envelope.bar_open_ms,
            bar_close_ms=envelope.bar_close_ms,
            decision_cutoff_ms=envelope.decision_cutoff_ms,
            family=envelope.family,
            readiness=envelope.readiness,
            direction=envelope.direction,
            strength_micros=envelope.strength_micros,
            producer_version_id=envelope.producer_version_id,
            source_lineage_root_sha256=envelope.source_lineage_root_sha256,
            feature_slice_root_sha256=envelope.feature_slice_root_sha256,
            producer_evidence_sha256=envelope.producer_evidence_sha256,
            dependency_claims=envelope.dependency_claims,
            latest_source_event_ms=envelope.latest_source_event_ms,
            latest_source_receipt_ms=envelope.latest_source_receipt_ms,
            closed_bar=envelope.closed_bar,
            causal_inputs_complete=envelope.causal_inputs_complete,
            reasons=envelope.reasons,
        )
    with pytest.raises(EvidenceProducerContractErrorV2, match="producer factory"):
        replace(envelope, direction=1, strength_micros=1)
    with pytest.raises(EvidenceProducerContractErrorV2, match="finalization"):
        EvidenceFamilyObservationV2(
            producer_envelope=envelope,
            ownership_scope_sha256=observation.ownership_scope_sha256,
            ownership_ledger_root_sha256=(
                observation.ownership_ledger_root_sha256
            ),
        )
    with pytest.raises(EvidenceProducerContractErrorV2, match="finalization"):
        replace(observation)


def test_five_of_six_cannot_finalize_and_failed_finalize_is_atomic() -> None:
    ledger = _empty_ledger()
    for family in FAMILIES[:5]:
        ledger.register(
            build_test_producer_envelope_v2(
                family=family,
                bar_open_ms=BAR_OPEN_MS,
                bar_close_ms=BAR_CLOSE_MS,
                decision_cutoff_ms=DECISION_CUTOFF_MS,
            )
        )
    before = (ledger.event_count, ledger.replay_root_sha256, ledger.export_state_v2())

    with pytest.raises(EvidenceProducerContractErrorV2, match="all six"):
        ledger.finalize_observations_v2()

    assert (ledger.event_count, ledger.replay_root_sha256, ledger.export_state_v2()) == before
    ledger.register(
        build_test_producer_envelope_v2(
            family=FAMILIES[5],
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
        )
    )
    assert len(ledger.finalize_observations_v2()) == 6


def test_duplicate_family_source_or_evidence_cannot_create_extra_votes() -> None:
    duplicate_slice = digest_test_value_v2("renamed-economic-slice")
    with pytest.raises(EvidenceProducerContractErrorV2, match="economic feature slice"):
        build_test_ownership_ledger_v2(
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            economic_slice_overrides={
                FAMILIES[0]: duplicate_slice,
                FAMILIES[1]: duplicate_slice,
            },
        )


def test_alias_rejection_is_atomic_but_shared_lineage_distinct_slices_pass() -> None:
    alias = digest_test_value_v2("same-economic-slice-different-name")
    shared_lineage = digest_test_value_v2("shared-kline-lineage")
    ledger = _empty_ledger()
    price = build_test_producer_envelope_v2(
        family=FAMILIES[0],
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        economic_slice_sha256=alias,
        source_lineage_root_sha256=shared_lineage,
    )
    volatility_alias = build_test_producer_envelope_v2(
        family=FAMILIES[2],
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        economic_slice_sha256=alias,
        source_lineage_root_sha256=shared_lineage,
    )
    ledger.register(price)
    before = (ledger.event_count, ledger.replay_root_sha256, ledger.export_state_v2())

    with pytest.raises(EvidenceProducerContractErrorV2, match="economic feature slice"):
        ledger.register(volatility_alias)
    assert (ledger.event_count, ledger.replay_root_sha256, ledger.export_state_v2()) == before

    shared = {family: shared_lineage for family in FAMILIES}
    valid = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        lineage_overrides=shared,
    )
    assert len(valid.finalize_observations_v2()) == 6

    duplicate_evidence = digest_test_value_v2("duplicate-producer-evidence")
    with pytest.raises(
        EvidenceProducerContractErrorV2,
        match="producer evidence document",
    ):
        build_test_ownership_ledger_v2(
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            producer_evidence_overrides={
                FAMILIES[0]: duplicate_evidence,
                FAMILIES[1]: duplicate_evidence,
            },
        )


def test_spot_provenance_is_rejected_before_scoring() -> None:
    with pytest.raises(EvidenceScoreContractErrorV2, match="USD-M Futures"):
        _input(venue=VenueV2.SPOT)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (EvidenceReadinessV2.FEATURE_NOT_READY, EvidenceReadinessV2.FEATURE_NOT_READY),
        (EvidenceReadinessV2.INCONCLUSIVE_DATA, EvidenceReadinessV2.INCONCLUSIVE_DATA),
        (EvidenceReadinessV2.DATA_INVALID, EvidenceReadinessV2.DATA_INVALID),
    ],
)
def test_any_nonready_family_withholds_the_entire_score(
    status: EvidenceReadinessV2,
    expected: EvidenceReadinessV2,
) -> None:
    statuses = (status, *(EvidenceReadinessV2.READY for _ in range(5)))
    decision = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                readiness=statuses,
            )
        )
    )

    assert decision.status is expected
    assert decision.bias is EvidenceBiasV2.NOT_AVAILABLE
    assert decision.evidence_score_micros is None
    assert decision.bullish_family_count is None


def test_nonready_precedence_is_invalid_then_inconclusive_then_not_ready() -> None:
    readiness = (
        EvidenceReadinessV2.FEATURE_NOT_READY,
        EvidenceReadinessV2.INCONCLUSIVE_DATA,
        EvidenceReadinessV2.DATA_INVALID,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
        EvidenceReadinessV2.READY,
    )
    decision = evaluate_evidence_score_v2(
        _input(observations=_observations(readiness=readiness))
    )

    assert decision.status is EvidenceReadinessV2.DATA_INVALID


def test_unclosed_and_incomplete_causal_input_fail_closed_without_partial_score() -> None:
    unclosed = evaluate_evidence_score_v2(_input(closed_bar=False))
    incomplete = evaluate_evidence_score_v2(_input(causal_inputs_complete=False))

    assert unclosed.status is EvidenceReadinessV2.DATA_INVALID
    assert incomplete.status is EvidenceReadinessV2.INCONCLUSIVE_DATA
    assert unclosed.evidence_score_micros is None
    assert incomplete.evidence_score_micros is None


def test_assembler_derives_flags_and_direct_false_to_true_override_is_rejected() -> None:
    unclosed_ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        closed_bar=False,
    )
    item = assemble_evidence_score_input_v2(unclosed_ledger)
    assert not item.closed_bar
    assert item.causal_inputs_complete
    assert evaluate_evidence_score_v2(item).status is EvidenceReadinessV2.DATA_INVALID

    with pytest.raises(EvidenceScoreContractErrorV2, match="closed_bar differs"):
        EvidenceScoreInputV2(
            attempt_id=item.attempt_id,
            symbol=item.symbol,
            venue=item.venue,
            promoting_plan_sha256=item.promoting_plan_sha256,
            bar_open_ms=item.bar_open_ms,
            bar_close_ms=item.bar_close_ms,
            decision_cutoff_ms=item.decision_cutoff_ms,
            closed_bar=True,
            causal_inputs_complete=item.causal_inputs_complete,
            observations=item.observations,
        )

    incomplete_ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        causal_inputs_complete=False,
    )
    incomplete = assemble_evidence_score_input_v2(incomplete_ledger)
    with pytest.raises(
        EvidenceScoreContractErrorV2,
        match="causal_inputs_complete differs",
    ):
        replace(incomplete, causal_inputs_complete=True)


def test_mixed_scope_or_ownership_root_observations_fail_closed() -> None:
    first = _observations((1, 0, 0, 0, 0, 0))
    changed_root = _observations((0, 1, 0, 0, 0, 0))
    mixed_root = (changed_root[0], *first[1:])
    with pytest.raises(EvidenceScoreContractErrorV2, match="ownership ledger root"):
        _input(observations=mixed_root)

    other_symbol = _observations(symbol="ETHUSDT")
    with pytest.raises(EvidenceScoreContractErrorV2, match="scope differs"):
        _input(observations=other_symbol)


def test_direct_ready_decision_cannot_bind_unclosed_or_incomplete_envelopes() -> None:
    ready = evaluate_evidence_score_v2(_input())
    unclosed = _observations(closed_bar=False)
    incomplete = _observations(causal_inputs_complete=False)

    with pytest.raises(EvidenceScoreContractErrorV2, match="closed_bar differs"):
        replace(ready, observations=unclosed)
    with pytest.raises(
        EvidenceScoreContractErrorV2,
        match="causal_inputs_complete differs",
    ):
        replace(ready, observations=incomplete)


def test_observation_order_accepts_post_close_through_d_and_rejects_invalid_bounds() -> None:
    equality = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                (1, 1, 1, 0, 0, 0),
                event_ms=BAR_CLOSE_MS,
                receipt_ms=DECISION_CUTOFF_MS,
            )
        )
    )
    post_close = evaluate_evidence_score_v2(
        _input(observations=_observations(event_ms=BAR_CLOSE_MS + 1))
    )
    at_cutoff = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                event_ms=DECISION_CUTOFF_MS,
                receipt_ms=DECISION_CUTOFF_MS,
            )
        )
    )
    preclose = _observations(event_ms=BAR_CLOSE_MS - 1)
    postreceipt = _observations(
        event_ms=DECISION_CUTOFF_MS,
        receipt_ms=DECISION_CUTOFF_MS - 1,
    )
    late = _observations(receipt_ms=DECISION_CUTOFF_MS + 1)
    incomplete_at_cutoff = evaluate_evidence_score_v2(
        _input(
            causal_inputs_complete=False,
            observations=_observations(
                receipt_ms=DECISION_CUTOFF_MS,
                causal_inputs_complete=False,
            ),
        )
    )
    incomplete_late = evaluate_evidence_score_v2(
        _input(
            causal_inputs_complete=False,
            observations=_observations(
                receipt_ms=DECISION_CUTOFF_MS + 1,
                causal_inputs_complete=False,
            ),
        )
    )
    unclosed_late = evaluate_evidence_score_v2(
        _input(
            closed_bar=False,
            observations=_observations(
                receipt_ms=DECISION_CUTOFF_MS + 1,
                closed_bar=False,
            ),
        )
    )

    assert equality.ready
    assert post_close.ready
    assert at_cutoff.ready
    assert (
        evaluate_evidence_score_v2(_input(observations=preclose)).status
        is EvidenceReadinessV2.DATA_INVALID
    )
    assert (
        evaluate_evidence_score_v2(_input(observations=postreceipt)).status
        is EvidenceReadinessV2.DATA_INVALID
    )
    assert (
        evaluate_evidence_score_v2(_input(observations=late)).status
        is EvidenceReadinessV2.DATA_INVALID
    )
    assert incomplete_at_cutoff.status is EvidenceReadinessV2.INCONCLUSIVE_DATA
    assert incomplete_late.status is EvidenceReadinessV2.DATA_INVALID
    assert any(
        reason.startswith("LATE_RECEIPT_FORBIDDEN:")
        for reason in incomplete_late.reasons
    )
    assert unclosed_late.status is EvidenceReadinessV2.DATA_INVALID
    assert unclosed_late.reasons == ("UNCLOSED_CANDLE_FORBIDDEN",)


def test_direct_decision_construction_rechecks_causal_observation_times() -> None:
    ready = evaluate_evidence_score_v2(_input())
    incomplete = evaluate_evidence_score_v2(
        _input(causal_inputs_complete=False)
    )
    feature_not_ready_readiness = (
        EvidenceReadinessV2.FEATURE_NOT_READY,
    ) * len(FAMILIES)
    feature_not_ready = evaluate_evidence_score_v2(
        _input(
            observations=_observations(
                readiness=feature_not_ready_readiness,
            )
        )
    )
    preclose = _observations(event_ms=ready.bar_close_ms - 1)
    postreceipt = _observations(
        event_ms=ready.decision_cutoff_ms,
        receipt_ms=ready.decision_cutoff_ms - 1,
    )
    late = _observations(receipt_ms=ready.decision_cutoff_ms + 1)
    incomplete_late = _observations(
        receipt_ms=ready.decision_cutoff_ms + 1,
        causal_inputs_complete=False,
    )
    feature_not_ready_late = _observations(
        readiness=feature_not_ready_readiness,
        receipt_ms=ready.decision_cutoff_ms + 1,
    )

    with pytest.raises(EvidenceScoreContractErrorV2, match=r"k\.T <="):
        replace(ready, observations=preclose)
    with pytest.raises(EvidenceScoreContractErrorV2, match=r"k\.T <="):
        replace(ready, observations=postreceipt)
    with pytest.raises(EvidenceScoreContractErrorV2, match=r"k\.T <="):
        replace(ready, observations=late)
    with pytest.raises(EvidenceScoreContractErrorV2, match="non-DATA_INVALID"):
        replace(incomplete, observations=incomplete_late)
    with pytest.raises(EvidenceScoreContractErrorV2, match="non-DATA_INVALID"):
        replace(feature_not_ready, observations=feature_not_ready_late)


def test_logical_event_id_excludes_payload_and_registry_detects_conflicts() -> None:
    first = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 0, 0, 0)))
    )
    identical = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 0, 0, 0)))
    )
    changed = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )
    registry = EvidenceAnnotationRegistryV2(maximum_events=2)

    assert first.event_id == changed.event_id
    assert first.payload_sha256 != changed.payload_sha256
    assert registry.register(first) is EvidenceRegistryDispositionV2.NEW
    assert (
        registry.register(identical)
        is EvidenceRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    with pytest.raises(EvidenceScoreContractErrorV2, match="collides"):
        registry.register(changed)


def test_full_ownership_ledger_exact_replay_is_idempotent_before_conflict() -> None:
    ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
    )
    observations = ledger.finalize_observations_v2()
    exact = build_test_producer_envelope_v2(
        family=FAMILIES[0],
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
    )
    conflict = build_test_producer_envelope_v2(
        family=FAMILIES[0],
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        direction=1,
    )

    assert (
        ledger.register(exact)
        is DependencyOwnershipDispositionV2.IDEMPOTENT_DUPLICATE
    )
    assert ledger.finalize_observations_v2() is observations
    with pytest.raises(EvidenceProducerContractErrorV2, match="conflicting"):
        ledger.register(conflict)


def test_ownership_restart_is_canonical_and_externally_pinned() -> None:
    ledger = build_test_ownership_ledger_v2(
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        directions=(1, 1, 1, 0, 0, 0),
    )
    ledger.finalize_observations_v2()
    payload = ledger.export_state_v2()
    restored = DependencyOwnershipLedgerV2.from_state_v2(
        payload,
        expected_replay_root_sha256=ledger.replay_root_sha256,
        expected_envelope_count=ledger.event_count,
        expected_maximum_families=ledger.maximum_families,
        expected_scope_sha256=ledger.scope_sha256,
        expected_finalized=True,
    )
    assert restored.export_state_v2() == payload
    assert restored.finalize_observations_v2() == ledger.finalize_observations_v2()

    prefix = _empty_ledger()
    for family in FAMILIES[:5]:
        prefix.register(
            build_test_producer_envelope_v2(
                family=family,
                bar_open_ms=BAR_OPEN_MS,
                bar_close_ms=BAR_CLOSE_MS,
                decision_cutoff_ms=DECISION_CUTOFF_MS,
            )
        )
    with pytest.raises(EvidenceProducerContractErrorV2, match="externally pinned"):
        DependencyOwnershipLedgerV2.from_state_v2(
            prefix.export_state_v2(),
            expected_replay_root_sha256=ledger.replay_root_sha256,
            expected_envelope_count=ledger.event_count,
            expected_maximum_families=ledger.maximum_families,
            expected_scope_sha256=ledger.scope_sha256,
            expected_finalized=True,
        )

    reordered = json.loads(payload)
    reordered["envelopes"] = list(reversed(reordered["envelopes"]))
    with pytest.raises(EvidenceProducerContractErrorV2, match="canonical family order"):
        DependencyOwnershipLedgerV2.from_state_v2(
            canonical_json_line(reordered),
            expected_replay_root_sha256=ledger.replay_root_sha256,
            expected_envelope_count=ledger.event_count,
            expected_maximum_families=ledger.maximum_families,
            expected_scope_sha256=ledger.scope_sha256,
            expected_finalized=True,
        )


def test_registry_is_bounded_and_fails_closed_instead_of_evicting_history() -> None:
    registry = EvidenceAnnotationRegistryV2(maximum_events=1)
    first = evaluate_evidence_score_v2(_input())
    second = evaluate_evidence_score_v2(
        _input(
            symbol="ETHUSDT",
            observations=_observations(
                (1, 0, 0, 0, 0, 0),
                symbol="ETHUSDT",
            ),
        )
    )

    assert registry.register(first) is EvidenceRegistryDispositionV2.NEW
    with pytest.raises(EvidenceScoreContractErrorV2, match="capacity exhausted"):
        registry.register(second)
    assert registry.event_count == 1


def test_annotation_evaluation_cannot_mutate_prospective_alpha_registry() -> None:
    registry = AttemptAlphaRegistryV2()
    before = registry.registered_nominal_alpha

    evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )

    assert registry.registered_nominal_alpha == before
    assert registry.attempts == ()


def test_canonical_payload_is_json_and_binds_every_material_score_field() -> None:
    decision = evaluate_evidence_score_v2(
        _input(observations=_observations((1, 1, 1, 1, 0, 0)))
    )
    document = json.loads(canonical_evidence_score_decision_v2(decision))

    assert document["payload_sha256"] == decision.payload_sha256
    assert document["event_id"] == decision.event_id
    assert document["promoting"] is False
    assert document["changes_primary_decision"] is False
    assert document["probability_calibrated"] is False
    assert document["score_numerator_micros"] == 4_000_000

    with pytest.raises(EvidenceScoreContractErrorV2, match="contradict"):
        replace(decision, score_numerator_micros=3_999_999)


def test_observation_contract_rejects_fake_neutral_and_nonready_strength() -> None:
    base = _observations()[0]
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        replace(base, direction=1)
    with pytest.raises(EvidenceProducerContractErrorV2, match="neutral evidence"):
        build_test_producer_envelope_v2(
            family=EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            direction=0,
            strength_micros=1,
        )
    with pytest.raises(EvidenceProducerContractErrorV2, match="non-ready evidence"):
        build_test_producer_envelope_v2(
            family=EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            readiness=EvidenceReadinessV2.FEATURE_NOT_READY,
            direction=1,
            strength_micros=1,
        )


@pytest.mark.parametrize("strength_micros", [-1, EVIDENCE_STRENGTH_SCALE_V2 + 1])
def test_producer_strength_bounds_reject_one_micro_outside(
    strength_micros: int,
) -> None:
    with pytest.raises(EvidenceProducerContractErrorV2, match=r"\[0, 1000000\]"):
        build_test_producer_envelope_v2(
            family=EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            direction=1,
            strength_micros=strength_micros,
        )
