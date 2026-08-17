from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBAdmissionDispositionV2,
    FamilyBAdmissionReceiptV2,
    FamilyBContractError,
    FamilyBDecisionRegistryV2,
    FamilyBEntryDecisionV2,
    FamilyBEntryInputV2,
    FamilyBExitDecisionV2,
    FamilyBExitDispositionV2,
    FamilyBExitMutationReceiptV2,
    FamilyBExitReasonV2,
    FamilyBSideV2,
    evaluate_family_b_entry_v2,
)

from .test_family_b import (
    PLAN_SHA,
    _entry_input,
    _exit_state,
    _paper_fill,
    _position_state,
    _registry,
)


def _admission_state() -> tuple[
    FamilyBEntryInputV2,
    FamilyBEntryDecisionV2,
    FamilyBDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokDecisionRegistryV2,
]:
    item = _entry_input(attempt_id="lifecycle-admission")
    registry = _registry()
    decision = evaluate_family_b_entry_v2(item, registry)
    paper_decision, certificate, paper_registry = _paper_fill(item, decision)
    return item, decision, registry, paper_decision, certificate, paper_registry


def test_admission_receipt_is_exact_idempotent_and_new_receipt_rolls_back() -> None:
    item, decision, registry, paper_decision, certificate, paper_registry = _admission_state()
    pre_root = registry.replay_root_sha256
    pre_count = registry.event_count
    receipt = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )

    assert receipt.disposition is FamilyBAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.pre_root_sha256 == pre_root
    assert receipt.pre_event_count == receipt.post_event_count == pre_count
    assert receipt.post_root_sha256 == registry.replay_root_sha256 != pre_root
    assert receipt.position.entry_event_id == decision.event_id
    assert receipt.paper_decision.event_id == receipt.position.paper_decision_event_id
    assert (
        receipt.paper_certificate.certificate_sha256 == receipt.position.admission_evidence_sha256
    )
    assert (
        receipt.paper_registry_checkpoint_sha256
        == receipt.position.paper_registry_checkpoint_sha256
    )

    replay = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert replay.disposition is FamilyBAdmissionDispositionV2.PREEXISTING
    assert replay.position is receipt.position
    assert replay.pre_root_sha256 == replay.post_root_sha256 == receipt.post_root_sha256
    with pytest.raises(FamilyBContractError, match="pre-existing"):
        registry.rollback_position_admission(
            item,
            decision,
            replay,
        )

    assert registry.rollback_position_admission(
        item,
        decision,
        receipt,
    )
    assert registry.replay_root_sha256 == pre_root
    assert registry.event_count == pre_count
    assert not registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )
    with pytest.raises(FamilyBContractError, match="does not own"):
        registry.rollback_position_admission(
            item,
            decision,
            receipt,
        )


def test_admission_receipt_rejects_foreign_tamper_and_post_admission_drift() -> None:
    item, decision, registry, paper_decision, certificate, paper_registry = _admission_state()
    receipt = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    foreign = _registry()
    with pytest.raises(FamilyBContractError, match="another registry"):
        foreign.rollback_position_admission(
            item,
            decision,
            receipt,
        )
    with pytest.raises(FamilyBContractError, match="must be created"):
        replace(receipt)

    exit_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(receipt.position, registry),
    )
    registry.evaluate_exit_with_receipt(exit_item)
    with pytest.raises(FamilyBContractError, match="state drifted"):
        registry.rollback_position_admission(
            item,
            decision,
            receipt,
        )

    object.__setattr__(receipt, "position_sha256", "f" * 64)
    with pytest.raises(FamilyBContractError, match="position hash differs"):
        registry.rollback_position_admission(
            item,
            decision,
            receipt,
        )


@pytest.mark.parametrize("close_price", [Decimal("100"), Decimal("90")])
def test_exit_receipt_rolls_back_hold_and_terminal_state_exactly(
    close_price: Decimal,
) -> None:
    position, registry = _position_state(FamilyBSideV2.LONG)
    item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(position, registry),
        close_price=close_price,
    )
    pre_root = registry.replay_root_sha256
    pre_count = registry.event_count
    receipt = registry.evaluate_exit_with_receipt(item)

    assert receipt.disposition is FamilyBExitDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.pre_root_sha256 == pre_root
    assert receipt.post_event_count == pre_count + 1
    assert receipt.post_root_sha256 == registry.replay_root_sha256 != pre_root
    assert receipt.post_terminal is receipt.decision.exits_position
    assert registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    ) is (not receipt.decision.exits_position)

    replay = registry.evaluate_exit_with_receipt(item)
    assert replay.disposition is FamilyBExitDispositionV2.PREEXISTING
    assert replay.decision is receipt.decision
    assert replay.pre_root_sha256 == replay.post_root_sha256 == receipt.post_root_sha256
    with pytest.raises(FamilyBContractError, match="pre-existing"):
        registry.rollback_exit(item, replay)

    assert registry.rollback_exit(item, receipt)
    assert registry.replay_root_sha256 == pre_root
    assert registry.event_count == pre_count
    assert registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )


def test_exit_receipt_rejects_foreign_tamper_drift_and_factory_forgery() -> None:
    position, registry = _position_state(FamilyBSideV2.LONG)
    item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(position, registry),
    )
    receipt = registry.evaluate_exit_with_receipt(item)
    foreign_position, foreign = _position_state(FamilyBSideV2.LONG)
    foreign_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(foreign_position, foreign),
    )
    with pytest.raises(FamilyBContractError, match="another registry"):
        foreign.rollback_exit(foreign_item, receipt)
    with pytest.raises(FamilyBContractError, match="must be created"):
        replace(receipt)

    later_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(position, registry),
        bar_open_ms=item.bar_open_ms + 300_000,
        bar_close_ms=item.bar_close_ms + 300_000,
        decision_cutoff_ms=item.decision_cutoff_ms + 300_000,
    )
    registry.evaluate_exit_with_receipt(later_item)
    with pytest.raises(FamilyBContractError, match="state drifted"):
        registry.rollback_exit(item, receipt)

    object.__setattr__(receipt, "input_sha256", "f" * 64)
    with pytest.raises(FamilyBContractError, match="exact input"):
        registry.rollback_exit(item, receipt)

    valid = receipt.decision
    with pytest.raises(FamilyBContractError, match="created by the evaluator"):
        FamilyBExitDecisionV2(
            entry_event_id=valid.entry_event_id,
            attempt_id=valid.attempt_id,
            symbol=valid.symbol,
            venue=valid.venue,
            promoting_plan_sha256=valid.promoting_plan_sha256,
            bar_open_ms=valid.bar_open_ms,
            bar_close_ms=valid.bar_close_ms,
            decision_cutoff_ms=valid.decision_cutoff_ms,
            position_side=valid.position_side,
            exit_evidence_sha256=valid.exit_evidence_sha256,
            exit_source_root_sha256=valid.exit_source_root_sha256,
            action=valid.action,
            reason=FamilyBExitReasonV2.HOLD,
            reasons=("HOLD",),
            invalidation=valid.invalidation,
        )


def test_lifecycle_mutations_require_held_prospective_authority() -> None:
    item = _entry_input(attempt_id="lifecycle-authority")
    registry = _registry()
    authority = registry._claim_prospective_decision_authority_v2()
    decision = registry.evaluate_entry(item, _prospective_authority=authority)
    paper_decision, certificate, paper_registry = _paper_fill(item, decision)
    with pytest.raises(FamilyBContractError, match="requires the held"):
        registry.admit_position_with_receipt(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )
    receipt = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
        _prospective_authority=authority,
    )
    exit_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=(receipt.position, registry),
    )
    with pytest.raises(FamilyBContractError, match="requires the held"):
        registry.evaluate_exit_with_receipt(exit_item)
    exit_receipt = registry.evaluate_exit_with_receipt(
        exit_item,
        _prospective_authority=authority,
    )
    assert isinstance(exit_receipt, FamilyBExitMutationReceiptV2)


def test_receipt_types_cannot_be_constructed_without_registry_factory() -> None:
    item, decision, registry, paper_decision, certificate, paper_registry = _admission_state()
    admission = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    with pytest.raises(FamilyBContractError, match="must be created"):
        FamilyBAdmissionReceiptV2(
            input_sha256=admission.input_sha256,
            decision=admission.decision,
            position=admission.position,
            position_sha256=admission.position_sha256,
            paper_decision=admission.paper_decision,
            paper_certificate=admission.paper_certificate,
            paper_registry_root_sha256=admission.paper_registry_root_sha256,
            paper_registry_event_count=admission.paper_registry_event_count,
            paper_registry_maximum_events=admission.paper_registry_maximum_events,
            paper_registry_checkpoint_sha256=(admission.paper_registry_checkpoint_sha256),
            pre_root_sha256=admission.pre_root_sha256,
            pre_event_count=admission.pre_event_count,
            post_root_sha256=admission.post_root_sha256,
            post_event_count=admission.post_event_count,
            disposition=admission.disposition,
            _owner_token=object(),
            _rollback_capability=object(),
        )
    assert isinstance(admission, FamilyBAdmissionReceiptV2)
