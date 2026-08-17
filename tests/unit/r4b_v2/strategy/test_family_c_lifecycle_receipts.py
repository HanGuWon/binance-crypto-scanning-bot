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
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCAdmissionDispositionV2,
    FamilyCAdmissionReceiptV2,
    FamilyCContractError,
    FamilyCEntryDecisionV2,
    FamilyCEntryInputV2,
    FamilyCEpisodeLedgerV2,
    FamilyCExitDispositionV2,
    FamilyCExitMutationReceiptV2,
    FamilyCSideV2,
    evaluate_family_c_entry_v2,
)

from .test_family_c import (
    PROMOTING_PLAN_SHA256,
    _complete_moves,
    _entry_input,
    _exit_input,
    _ledger,
    _paper_admission,
    _position,
)


def _admission_state() -> tuple[
    FamilyCEntryInputV2,
    FamilyCEntryDecisionV2,
    FamilyCEpisodeLedgerV2,
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokDecisionRegistryV2,
]:
    item = _entry_input(attempt_id="lifecycle-c-admission")
    ledger = _ledger()
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    return item, decision, ledger, paper_decision, certificate, paper_registry


def test_admission_receipt_is_exact_idempotent_and_new_receipt_rolls_back() -> None:
    item, decision, ledger, paper_decision, certificate, paper_registry = _admission_state()
    pre_root = ledger.root_sha256
    pre_count = ledger.event_count
    receipt = ledger.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )

    assert receipt.disposition is FamilyCAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.pre_root_sha256 == pre_root
    assert receipt.pre_event_count == receipt.post_event_count == pre_count
    assert receipt.post_root_sha256 == ledger.root_sha256 != pre_root
    assert receipt.position.entry_event_id == decision.event_id
    assert receipt.paper_decision.event_id == receipt.position.paper_decision_event_id
    assert (
        receipt.paper_certificate.certificate_sha256 == receipt.position.admission_evidence_sha256
    )

    replay = ledger.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert replay.disposition is FamilyCAdmissionDispositionV2.PREEXISTING
    assert replay.position is receipt.position
    assert replay.pre_root_sha256 == replay.post_root_sha256 == receipt.post_root_sha256
    with pytest.raises(FamilyCContractError, match="pre-existing"):
        ledger.rollback_position_admission(item, decision, replay)

    assert ledger.rollback_position_admission(item, decision, receipt)
    assert ledger.root_sha256 == pre_root
    assert ledger.event_count == pre_count
    assert not ledger.is_active(
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        venue=VenueV2.USDM_FUTURES,
        symbol=item.target_symbol,
    )
    with pytest.raises(FamilyCContractError, match="does not own"):
        ledger.rollback_position_admission(item, decision, receipt)


def test_admission_receipt_rejects_foreign_tamper_and_exit_drift() -> None:
    item, decision, ledger, paper_decision, certificate, paper_registry = _admission_state()
    receipt = ledger.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    with pytest.raises(FamilyCContractError, match="another ledger"):
        _ledger().rollback_position_admission(item, decision, receipt)
    with pytest.raises(FamilyCContractError, match="must be created"):
        replace(receipt)

    exit_item, _ = _exit_input(position_state=(receipt.position, ledger))
    ledger.evaluate_exit_with_receipt(exit_item)
    with pytest.raises(FamilyCContractError, match="state drifted"):
        ledger.rollback_position_admission(item, decision, receipt)

    object.__setattr__(receipt, "paper_registry_checkpoint_sha256", "f" * 64)
    with pytest.raises(FamilyCContractError, match="evidence differs"):
        ledger.rollback_position_admission(item, decision, receipt)


@pytest.mark.parametrize("target_move", [Decimal("0"), Decimal("-1")])
def test_exit_receipt_rolls_back_hold_and_terminal_state_exactly(
    target_move: Decimal,
) -> None:
    position, ledger = _position()
    item, _ = _exit_input(
        position_state=(position, ledger),
        target_move=target_move,
    )
    pre_root = ledger.root_sha256
    pre_count = ledger.event_count
    receipt = ledger.evaluate_exit_with_receipt(item)

    assert receipt.disposition is FamilyCExitDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.pre_next_horizon == 1
    assert receipt.post_next_horizon == (1 if receipt.decision.exits_position else 2)
    assert receipt.post_event_count == pre_count + 1
    assert receipt.post_root_sha256 == ledger.root_sha256 != pre_root
    assert receipt.post_terminal is receipt.decision.exits_position

    replay = ledger.evaluate_exit_with_receipt(item)
    assert replay.disposition is FamilyCExitDispositionV2.PREEXISTING
    assert replay.decision is receipt.decision
    with pytest.raises(FamilyCContractError, match="pre-existing"):
        ledger.rollback_exit(item, replay)

    assert ledger.rollback_exit(item, receipt)
    assert ledger.root_sha256 == pre_root
    assert ledger.event_count == pre_count
    assert ledger.is_active(
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        venue=VenueV2.USDM_FUTURES,
        symbol=position.symbol,
    )
    recommit = ledger.evaluate_exit_with_receipt(item)
    assert recommit.pre_next_horizon == 1


def test_exit_rollback_restores_sticky_horizon_and_refuses_later_drift() -> None:
    position, ledger = _position()
    incomplete, _ = _exit_input(
        position_state=(position, ledger),
        member_moves=_complete_moves()[:-1],
    )
    receipt = ledger.evaluate_exit_with_receipt(incomplete)
    assert receipt.post_sticky_inconclusive
    assert receipt.post_next_horizon == 2
    assert ledger.rollback_exit(incomplete, receipt)

    first, _ = _exit_input(position_state=(position, ledger))
    first_receipt = ledger.evaluate_exit_with_receipt(first)
    assert not first_receipt.post_sticky_inconclusive
    second, _ = _exit_input(horizon=2, position_state=(position, ledger))
    ledger.evaluate_exit_with_receipt(second)
    with pytest.raises(FamilyCContractError, match="state drifted"):
        ledger.rollback_exit(first, first_receipt)


def test_exit_receipt_rejects_foreign_tamper_factory_forgery_and_capacity_edge() -> None:
    position, ledger = _position()
    item, _ = _exit_input(position_state=(position, ledger))
    receipt = ledger.evaluate_exit_with_receipt(item)
    foreign_position, foreign = _position()
    foreign_item, _ = _exit_input(position_state=(foreign_position, foreign))
    with pytest.raises(FamilyCContractError, match="another ledger"):
        foreign.rollback_exit(foreign_item, receipt)
    with pytest.raises(FamilyCContractError, match="must be created"):
        replace(receipt)
    object.__setattr__(receipt, "post_next_horizon", 99)
    with pytest.raises(FamilyCContractError, match="state transition"):
        ledger.rollback_exit(item, receipt)

    small = _ledger(maximum_events=2)
    entry = _entry_input(attempt_id="lifecycle-c-capacity")
    decision = evaluate_family_c_entry_v2(entry, small)
    paper_decision, certificate, paper_registry = _paper_admission(entry, decision)
    admission = small.admit_position_with_receipt(
        entry,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    first, _ = _exit_input(position_state=(admission.position, small))
    edge = small.evaluate_exit_with_receipt(first)
    assert edge.post_event_count == small.maximum_events
    second, _ = _exit_input(horizon=2, position_state=(admission.position, small))
    with pytest.raises(FamilyCContractError, match="capacity exhausted"):
        small.evaluate_exit_with_receipt(second)


def test_lifecycle_mutations_require_held_prospective_authority() -> None:
    item = _entry_input(attempt_id="lifecycle-c-authority")
    ledger = _ledger()
    authority = ledger._claim_prospective_decision_authority_v2()
    decision = ledger.evaluate_entry(item, _prospective_authority=authority)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    with pytest.raises(FamilyCContractError, match="requires the held"):
        ledger.admit_position_with_receipt(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )
    admission = ledger.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
        _prospective_authority=authority,
    )
    exit_item, _ = _exit_input(position_state=(admission.position, ledger))
    with pytest.raises(FamilyCContractError, match="requires the held"):
        ledger.evaluate_exit_with_receipt(exit_item)
    exit_receipt = ledger.evaluate_exit_with_receipt(
        exit_item,
        _prospective_authority=authority,
    )
    assert isinstance(exit_receipt, FamilyCExitMutationReceiptV2)
    assert isinstance(admission, FamilyCAdmissionReceiptV2)
    assert admission.position.side is FamilyCSideV2.LONG
