from __future__ import annotations

import json
from dataclasses import dataclass, replace
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.fees import FeeMultiplierV2
from signalbot.r4b_v2.execution.mandatory_exit import MandatoryExitPositionSideV2
from signalbot.r4b_v2.execution.paper_fok import PaperFokFullFillCertificateV2
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    size_fixed_quote_paper_entry_v2,
)
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
)
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    ProspectiveDailyWalStoreV2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveOutcomeWalRecordKindV2,
    build_prospective_outcome_wal_record_v2,
    prospective_outcome_id_v2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    ProspectivePaperTerminalPayloadV2,
    build_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_position_terminal_payload import (
    PROSPECTIVE_POSITION_TERMINAL_AUTHORITY_V2,
    FamilyLifecycleEvidenceReferenceV2,
    FinalFeeEvidenceReferenceV2,
    FundingCensusEvidenceReferenceV2,
    MandatoryExitEvidenceReferenceV2,
    PositionEvidenceReferenceAuthorityV2,
    ProspectivePositionTerminalContractErrorV2,
    ProspectivePositionTerminalStatusV2,
    build_family_a_lifecycle_evidence_reference_v2,
    build_family_b_lifecycle_evidence_reference_v2,
    build_family_c_lifecycle_evidence_reference_v2,
    build_final_fee_evidence_reference_v2,
    build_funding_census_evidence_reference_v2,
    build_mandatory_exit_evidence_reference_v2,
    build_prospective_position_terminal_payload_v2,
    canonical_prospective_position_terminal_payload_v2,
    parse_prospective_position_terminal_payload_v2,
    prospective_position_id_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAEntryDecisionV2,
    FamilyAEntryInputV2,
    FamilyAEpisodeLedgerV2,
)
from signalbot.r4b_v2.strategy.family_b import FamilyBSideV2

from ..strategy import test_family_a as family_a_testkit
from ..strategy import test_family_b as family_b_testkit
from ..strategy import test_family_b_lifecycle_receipts as family_b_receipt_testkit
from ..strategy import test_family_c as family_c_testkit
from ..strategy import test_family_c_lifecycle_receipts as family_c_receipt_testkit
from . import test_prospective_paper_terminal_payload as paper_terminal_testkit

_JCS_MAX_SAFE_INTEGER = 9_007_199_254_740_991


@dataclass(frozen=True, slots=True)
class _OpenedSources:
    plan: ProspectiveCensusPlanV2
    cell: ProspectiveExpectedCellV2
    paper_terminal: ProspectivePaperTerminalPayloadV2
    certificate: PaperFokFullFillCertificateV2
    family: FamilyLifecycleEvidenceReferenceV2
    mandatory: MandatoryExitEvidenceReferenceV2
    fee: FinalFeeEvidenceReferenceV2
    funding: FundingCensusEvidenceReferenceV2
    finalization_ms: int
    store: ProspectiveDailyWalStoreV2
    lease: WriterLease

    @property
    def kwargs(self) -> dict[str, object]:
        return {
            "plan": self.plan,
            "cell": self.cell,
            "sizing_cell": PaperSizingCellV2.NOTIONAL_100_USDT,
            "paper_terminal": self.paper_terminal,
            "finalized_at_ms": self.finalization_ms,
            "paper_terminal_record_sha256": "a" * 64,
            "full_fill_certificate": self.certificate,
            "family_evidence": self.family,
            "mandatory_exit_evidence": self.mandatory,
            "fee_evidence": self.fee,
            "funding_evidence": self.funding,
        }


def _opened_sources(
    tmp_path: Path,
    *,
    side: MandatoryExitPositionSideV2,
    exit_notional_delta: Decimal = Decimal("-5"),
) -> _OpenedSources:
    evidence = (
        family_a_testkit._short_crowd_evidence()  # pyright: ignore[reportPrivateUsage]
        if side is MandatoryExitPositionSideV2.LONG
        else family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    item: FamilyAEntryInputV2 = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        evidence
    )
    (
        plan,
        cell,
        transaction,
        store,
        lease,
    ) = paper_terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        item,
    )
    assert type(transaction.decision) is FamilyAEntryDecisionV2
    paper_decision, certificate, paper_registry = family_a_testkit._paper_full_fill(  # pyright: ignore[reportPrivateUsage]
        transaction.decision
    )
    sizing = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("50"),
        reference_evidence_sha256=paper_decision.evidence_sha256,
        quantity_grid=paper_terminal_testkit._grid(),  # pyright: ignore[reportPrivateUsage]
    )
    paper_terminal = build_prospective_paper_terminal_payload_v2(
        plan=plan,
        cell=cell,
        transaction=transaction,
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        sizing=sizing,
        paper_decision=paper_decision,
        full_fill_certificate=certificate,
    )
    ledger = FamilyAEpisodeLedgerV2(maximum_events=20)
    exact_signal = ledger.evaluate_entry(item)
    assert exact_signal == transaction.decision
    admission = ledger.admit_external_full_fill_with_receipt(
        item,
        exact_signal,
        paper_decision,
        certificate,
        paper_registry,
    )
    exit_input = family_a_testkit._exit_input_for_admission(  # pyright: ignore[reportPrivateUsage]
        admission,
        close_price=(Decimal("89") if side is MandatoryExitPositionSideV2.LONG else Decimal("111")),
    )
    terminal_exit = ledger.evaluate_exit_with_receipt(exit_input)
    assert terminal_exit.decision.exits_position
    outcome_id = prospective_outcome_id_v2(
        attempt_plan_sha256=plan.plan_sha256,
        origin_segment_id=cell.segment_id,
        origin_cell_id=cell.cell_id,
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
    )
    position_id = prospective_position_id_v2(
        outcome_id=outcome_id,
        certificate_sha256=certificate.certificate_sha256,
    )
    family = build_family_a_lifecycle_evidence_reference_v2(
        position_id=position_id,
        admission=admission,
        terminal_exit=terminal_exit,
    )
    terminal_at_ms = terminal_exit.decision.decision_cutoff_ms + 10_000
    exit_notional = certificate.executable_notional + exit_notional_delta
    signed_exit = exit_notional if side is MandatoryExitPositionSideV2.LONG else -exit_notional
    mandatory = build_mandatory_exit_evidence_reference_v2(
        attempt_id=plan.attempt_id,
        promoting_plan_sha256=plan.promoting_plan_sha256,
        symbol=cell.symbol,
        position_id=position_id,
        family_exit_event_id=family.exit_event_id,
        side=side,
        mandatory_position_sha256="1" * 64,
        exit_intent_sha256="2" * 64,
        target_cursor_sha256="3" * 64,
        terminal_sha256="4" * 64,
        terminal_payload_sha256="5" * 64,
        fee_certificate_sha256="6" * 64,
        ledger_checkpoint_sha256="7" * 64,
        exit_slices_root_sha256="8" * 64,
        exit_slice_count=2,
        filled_quantity=certificate.filled_quantity,
        residual_quantity=Decimal(0),
        gross_exit_notional_usdt=exit_notional,
        signed_exit_cashflow_usdt=signed_exit,
        terminal_at_ms=terminal_at_ms,
    )
    entry_fee = Decimal("0.050")
    exit_fee = Decimal("0.045")
    fee = build_final_fee_evidence_reference_v2(
        attempt_id=plan.attempt_id,
        promoting_plan_sha256=plan.promoting_plan_sha256,
        symbol=cell.symbol,
        position_id=position_id,
        mandatory_exit_fee_certificate_sha256=mandatory.fee_certificate_sha256,
        final_timeline_checkpoint_sha256="9" * 64,
        final_timeline_root_sha256="b" * 64,
        fee_position_payload_sha256="c" * 64,
        exit_slices_root_sha256=mandatory.exit_slices_root_sha256,
        exit_slice_count=mandatory.exit_slice_count,
        multiplier=FeeMultiplierV2.PRIMARY_1_0X,
        entry_fee_usdt=entry_fee,
        exit_fee_usdt=exit_fee,
        total_fee_usdt=entry_fee + exit_fee,
    )
    assert paper_terminal.target_venue_ms is not None
    funding = build_funding_census_evidence_reference_v2(
        attempt_id=plan.attempt_id,
        promoting_plan_sha256=plan.promoting_plan_sha256,
        symbol=cell.symbol,
        position_id=position_id,
        census_certificate_sha256="d" * 64,
        registry_checkpoint_sha256="e" * 64,
        position_ledger_checkpoint_sha256="f" * 64,
        cashflow_root_sha256="0" * 64,
        expected_funding_count=1,
        confirmed_funding_count=1,
        cashflow_event_count=1,
        interval_start_ms=paper_terminal.target_venue_ms,
        interval_end_ms=terminal_at_ms,
        observed_through_ms=terminal_at_ms,
        realized_funding_cashflow_usdt=Decimal("0.10"),
    )
    return _OpenedSources(
        plan=plan,
        cell=cell,
        paper_terminal=paper_terminal,
        certificate=certificate,
        family=family,
        mandatory=mandatory,
        fee=fee,
        funding=funding,
        finalization_ms=terminal_at_ms,
        store=store,
        lease=lease,
    )


def _close(sources: _OpenedSources) -> None:
    paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
        sources.store,
        sources.lease,
    )


@pytest.mark.parametrize(
    "side",
    [MandatoryExitPositionSideV2.LONG, MandatoryExitPositionSideV2.SHORT],
)
def test_complete_long_and_short_use_exact_cashflows_without_double_slippage(
    tmp_path: Path,
    side: MandatoryExitPositionSideV2,
) -> None:
    sources = _opened_sources(tmp_path, side=side)
    try:
        payload = build_prospective_position_terminal_payload_v2(**sources.kwargs)  # type: ignore[arg-type]
        encoded = canonical_prospective_position_terminal_payload_v2(payload)
        entry_notional = sources.certificate.executable_notional
        expected_entry = (
            entry_notional if side is MandatoryExitPositionSideV2.SHORT else -entry_notional
        )
        expected_gross = expected_entry + sources.mandatory.signed_exit_cashflow_usdt
        expected_after = (
            expected_gross
            + sources.funding.realized_funding_cashflow_usdt
            - sources.fee.total_fee_usdt
        )
        with localcontext(protocol_decimal_context_v2()):
            expected_return = expected_after / entry_notional

        assert payload.terminal_status is (ProspectivePositionTerminalStatusV2.COMPLETE_CALCULATION)
        assert payload.signed_entry_cashflow_usdt == expected_entry
        assert payload.gross_pnl_usdt == expected_gross
        assert payload.after_cost_pnl_usdt == expected_after
        assert payload.after_cost_return == expected_return
        assert payload.total_fee_usdt == Decimal("0.095")
        assert payload.diagnostic_entry_slippage_usdt is not None
        assert not payload.slippage_double_counted
        assert payload.authority_status == PROSPECTIVE_POSITION_TERMINAL_AUTHORITY_V2
        assert payload.position_terminal
        assert not payload.position_terminal_authoritative
        assert not payload.typed_wal_replay_authoritative
        assert not payload.terminal_rule_plan_bound
        assert not payload.production_order_placement
        assert not payload.actual_private_account_fee_claim
        assert not payload.efficacy_eligible
        assert len(encoded) < MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2

        document = json.loads(encoded)
        assert isinstance(document["gross_pnl_usdt"], str)
        assert isinstance(document["after_cost_return"], str)
        assert "canonical_paper" not in document
        assert document["family_evidence"]["reference_sha256"] == (sources.family.reference_sha256)
        record = build_prospective_outcome_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL,
            attempt_plan_sha256=sources.plan.plan_sha256,
            origin_segment_id=sources.cell.segment_id,
            origin_cell_id=sources.cell.cell_id,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            payload_schema=POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=encoded,
            previous_record_sha256=None,
        )
        assert record.outcome_id == payload.outcome_id
        assert (
            parse_prospective_position_terminal_payload_v2(
                encoded,
                **sources.kwargs,  # type: ignore[arg-type]
            )
            == payload
        )
    finally:
        _close(sources)


def test_negative_after_cost_is_a_valid_complete_numeric_result(tmp_path: Path) -> None:
    sources = _opened_sources(
        tmp_path,
        side=MandatoryExitPositionSideV2.LONG,
        exit_notional_delta=Decimal("-20"),
    )
    try:
        payload = build_prospective_position_terminal_payload_v2(**sources.kwargs)  # type: ignore[arg-type]
        assert payload.terminal_status is (ProspectivePositionTerminalStatusV2.COMPLETE_CALCULATION)
        assert payload.gross_pnl_usdt == Decimal("-20")
        assert payload.after_cost_pnl_usdt == Decimal("-19.995")
        assert payload.after_cost_return is not None
        assert payload.after_cost_return < 0
    finally:
        _close(sources)


def test_missing_sources_fail_closed_with_null_final_pnl(tmp_path: Path) -> None:
    sources = _opened_sources(tmp_path, side=MandatoryExitPositionSideV2.SHORT)
    try:
        payload = build_prospective_position_terminal_payload_v2(
            plan=sources.plan,
            cell=sources.cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=sources.paper_terminal,
            finalized_at_ms=sources.finalization_ms,
            full_fill_certificate=sources.certificate,
        )
        assert payload.terminal_status is ProspectivePositionTerminalStatusV2.INCOMPLETE
        assert payload.position_opened
        assert payload.entry_executable_notional_usdt == (sources.certificate.executable_notional)
        assert payload.gross_pnl_usdt is None
        assert payload.total_fee_usdt is None
        assert payload.realized_funding_cashflow_usdt is None
        assert payload.after_cost_pnl_usdt is None
        assert payload.after_cost_return is None
        assert not payload.costs_complete
        assert not payload.arithmetic_complete
        assert payload.reasons == (
            "MISSING_DURABLE_PAPER_TERMINAL_RECORD",
            "MISSING_FAMILY_LIFECYCLE_EVIDENCE",
            "MISSING_MANDATORY_EXIT_EVIDENCE",
            "MISSING_FINAL_FEE_EVIDENCE",
            "MISSING_FUNDING_CENSUS_EVIDENCE",
        )
    finally:
        _close(sources)


def test_contradictory_exit_fee_funding_and_family_evidence_are_rejected(
    tmp_path: Path,
) -> None:
    sources = _opened_sources(tmp_path, side=MandatoryExitPositionSideV2.SHORT)
    try:
        common = {
            "attempt_id": sources.plan.attempt_id,
            "promoting_plan_sha256": sources.plan.promoting_plan_sha256,
            "symbol": sources.cell.symbol,
            "position_id": sources.family.position_id,
        }
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="signed cashflow contradicts",
        ):
            build_mandatory_exit_evidence_reference_v2(
                **common,  # type: ignore[arg-type]
                family_exit_event_id=sources.family.exit_event_id,
                side=MandatoryExitPositionSideV2.SHORT,
                mandatory_position_sha256="1" * 64,
                exit_intent_sha256="2" * 64,
                target_cursor_sha256="3" * 64,
                terminal_sha256="4" * 64,
                terminal_payload_sha256="5" * 64,
                fee_certificate_sha256="6" * 64,
                ledger_checkpoint_sha256="7" * 64,
                exit_slices_root_sha256="8" * 64,
                exit_slice_count=1,
                filled_quantity=sources.certificate.filled_quantity,
                residual_quantity=Decimal(0),
                gross_exit_notional_usdt=Decimal("90"),
                signed_exit_cashflow_usdt=Decimal("90"),
                terminal_at_ms=sources.finalization_ms,
            )
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="total fee differs",
        ):
            build_final_fee_evidence_reference_v2(
                **common,  # type: ignore[arg-type]
                mandatory_exit_fee_certificate_sha256="6" * 64,
                final_timeline_checkpoint_sha256="9" * 64,
                final_timeline_root_sha256="b" * 64,
                fee_position_payload_sha256="c" * 64,
                exit_slices_root_sha256="8" * 64,
                exit_slice_count=1,
                multiplier=FeeMultiplierV2.PRIMARY_1_0X,
                entry_fee_usdt=Decimal("0.05"),
                exit_fee_usdt=Decimal("0.04"),
                total_fee_usdt=Decimal("0.10"),
            )
        assert sources.paper_terminal.target_venue_ms is not None
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="counts differ",
        ):
            build_funding_census_evidence_reference_v2(
                **common,  # type: ignore[arg-type]
                census_certificate_sha256="d" * 64,
                registry_checkpoint_sha256="e" * 64,
                position_ledger_checkpoint_sha256="f" * 64,
                cashflow_root_sha256="0" * 64,
                expected_funding_count=2,
                confirmed_funding_count=1,
                cashflow_event_count=1,
                interval_start_ms=sources.paper_terminal.target_venue_ms,
                interval_end_ms=sources.finalization_ms,
                observed_through_ms=sources.finalization_ms,
                realized_funding_cashflow_usdt=Decimal(0),
            )

        object.__setattr__(sources.family, "position_id", "f" * 64)
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="reference hash differs",
        ):
            build_prospective_position_terminal_payload_v2(
                **sources.kwargs  # type: ignore[arg-type]
            )
    finally:
        _close(sources)


def test_hold_receipt_cannot_pose_as_terminal_family_exit(tmp_path: Path) -> None:
    sources = _opened_sources(tmp_path, side=MandatoryExitPositionSideV2.SHORT)
    try:
        item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
            family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
        )
        ledger = FamilyAEpisodeLedgerV2(maximum_events=20)
        signal = ledger.evaluate_entry(item)
        paper_decision, certificate, paper_registry = family_a_testkit._paper_full_fill(  # pyright: ignore[reportPrivateUsage]
            signal
        )
        admission = ledger.admit_external_full_fill_with_receipt(
            item,
            signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        hold = ledger.evaluate_exit_with_receipt(
            family_a_testkit._exit_input_for_admission(  # pyright: ignore[reportPrivateUsage]
                admission
            )
        )
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="not the terminal exit",
        ):
            build_family_a_lifecycle_evidence_reference_v2(
                position_id=sources.family.position_id,
                admission=admission,
                terminal_exit=hold,
            )
    finally:
        _close(sources)


def test_family_b_reference_requires_exact_admission_and_terminal_exit_receipts() -> None:
    (
        item,
        decision,
        registry,
        paper_decision,
        certificate,
        paper_registry,
    ) = family_b_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
    admission = registry.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    terminal_item, _ = family_b_testkit._exit_state(  # pyright: ignore[reportPrivateUsage]
        admission.position.side,
        position_state=(admission.position, registry),
        close_price=(
            Decimal("90") if admission.position.side is FamilyBSideV2.LONG else Decimal("110")
        ),
    )
    terminal = registry.evaluate_exit_with_receipt(terminal_item)
    assert terminal.decision.exits_position
    reference = build_family_b_lifecycle_evidence_reference_v2(
        position_id="1" * 64,
        admission=admission,
        terminal_exit=terminal,
    )
    assert reference.family.value == "B"
    assert reference.entry_event_id == admission.decision.event_id
    assert reference.exit_event_id == terminal.decision.event_id
    assert reference.full_fill_certificate_sha256 == certificate.certificate_sha256
    assert reference.source_authority is (
        PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B_RECEIPTS
    )
    assert reference.admission_post_event_count == reference.admission_pre_event_count

    (
        hold_item,
        hold_decision,
        hold_registry,
        hold_paper,
        hold_certificate,
        hold_paper_registry,
    ) = family_b_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
    hold_admission = hold_registry.admit_position_with_receipt(
        hold_item,
        hold_decision,
        paper_decision=hold_paper,
        certificate=hold_certificate,
        paper_registry=hold_paper_registry,
    )
    hold_input, _ = family_b_testkit._exit_state(  # pyright: ignore[reportPrivateUsage]
        hold_admission.position.side,
        position_state=(hold_admission.position, hold_registry),
        close_price=Decimal("100"),
    )
    hold = hold_registry.evaluate_exit_with_receipt(hold_input)
    assert not hold.decision.exits_position
    with pytest.raises(
        ProspectivePositionTerminalContractErrorV2,
        match="not the terminal exit",
    ):
        build_family_b_lifecycle_evidence_reference_v2(
            position_id="1" * 64,
            admission=hold_admission,
            terminal_exit=hold,
        )

    object.__setattr__(terminal, "entry_event_id", "f" * 64)
    with pytest.raises(
        ProspectivePositionTerminalContractErrorV2,
        match="not the terminal exit",
    ):
        build_family_b_lifecycle_evidence_reference_v2(
            position_id="1" * 64,
            admission=admission,
            terminal_exit=terminal,
        )


def test_family_c_reference_requires_exact_admission_and_terminal_exit_receipts() -> None:
    (
        item,
        decision,
        ledger,
        paper_decision,
        certificate,
        paper_registry,
    ) = family_c_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
    admission = ledger.admit_position_with_receipt(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    terminal_input, _ = family_c_testkit._exit_input(  # pyright: ignore[reportPrivateUsage]
        position_state=(admission.position, ledger),
        target_move=Decimal("-1"),
    )
    terminal = ledger.evaluate_exit_with_receipt(terminal_input)
    assert terminal.decision.exits_position
    reference = build_family_c_lifecycle_evidence_reference_v2(
        position_id="2" * 64,
        admission=admission,
        terminal_exit=terminal,
    )
    assert reference.family.value == "C"
    assert reference.entry_event_id == admission.decision.event_id
    assert reference.exit_event_id == terminal.decision.event_id
    assert reference.full_fill_certificate_sha256 == certificate.certificate_sha256
    assert reference.source_authority is (
        PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C_RECEIPTS
    )
    assert reference.admission_post_event_count == reference.admission_pre_event_count

    (
        hold_item,
        hold_decision,
        hold_ledger,
        hold_paper,
        hold_certificate,
        hold_paper_registry,
    ) = family_c_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
    hold_admission = hold_ledger.admit_position_with_receipt(
        hold_item,
        hold_decision,
        paper_decision=hold_paper,
        certificate=hold_certificate,
        paper_registry=hold_paper_registry,
    )
    hold_input, _ = family_c_testkit._exit_input(  # pyright: ignore[reportPrivateUsage]
        position_state=(hold_admission.position, hold_ledger),
        target_move=Decimal(0),
    )
    hold = hold_ledger.evaluate_exit_with_receipt(hold_input)
    assert not hold.decision.exits_position
    with pytest.raises(
        ProspectivePositionTerminalContractErrorV2,
        match="not the terminal exit",
    ):
        build_family_c_lifecycle_evidence_reference_v2(
            position_id="2" * 64,
            admission=hold_admission,
            terminal_exit=hold,
        )

    with pytest.raises(
        ProspectivePositionTerminalContractErrorV2,
        match="exact FamilyBAdmissionReceiptV2",
    ):
        build_family_b_lifecycle_evidence_reference_v2(
            position_id="2" * 64,
            admission=admission,  # type: ignore[arg-type]
            terminal_exit=terminal,  # type: ignore[arg-type]
        )


def test_no_position_is_suppressed_with_exact_zero_cashflows(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=Decimal("1.49")
        )
    )
    (
        plan,
        cell,
        transaction,
        store,
        lease,
    ) = paper_terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        item,
    )
    try:
        paper_terminal = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        )
        payload = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=cell.decision_cutoff_ms,
            paper_terminal_record_sha256="a" * 64,
        )
        assert payload.terminal_status is (
            ProspectivePositionTerminalStatusV2.SUPPRESSED_NO_POSITION
        )
        assert not payload.position_opened
        assert payload.position_id is None
        assert payload.signed_entry_cashflow_usdt == Decimal(0)
        assert payload.signed_exit_cashflow_usdt == Decimal(0)
        assert payload.gross_pnl_usdt == Decimal(0)
        assert payload.total_fee_usdt == Decimal(0)
        assert payload.realized_funding_cashflow_usdt == Decimal(0)
        assert payload.after_cost_pnl_usdt == Decimal(0)
        assert payload.after_cost_return is None
        assert payload.costs_complete and payload.arithmetic_complete
    finally:
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            store,
            lease,
        )


def test_factory_tamper_unknown_field_and_safe_integer_boundary(tmp_path: Path) -> None:
    sources = _opened_sources(tmp_path, side=MandatoryExitPositionSideV2.SHORT)
    try:
        boundary_kwargs = {**sources.kwargs, "finalized_at_ms": _JCS_MAX_SAFE_INTEGER}
        payload = build_prospective_position_terminal_payload_v2(  # type: ignore[arg-type]
            **boundary_kwargs
        )
        encoded = canonical_prospective_position_terminal_payload_v2(payload)
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="factory-sealed",
        ):
            replace(payload)
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="RFC8785-safe",
        ):
            build_prospective_position_terminal_payload_v2(
                **{**sources.kwargs, "finalized_at_ms": _JCS_MAX_SAFE_INTEGER + 1}  # type: ignore[arg-type]
            )

        document = json.loads(encoded)
        document["after_cost_pnl_usdt"] = "123"
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="differs from its exact typed sources",
        ):
            parse_prospective_position_terminal_payload_v2(
                canonical_json_line(document),
                **boundary_kwargs,  # type: ignore[arg-type]
            )
        document["unknown"] = True
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="schema",
        ):
            parse_prospective_position_terminal_payload_v2(
                canonical_json_line(document),
                **boundary_kwargs,  # type: ignore[arg-type]
            )

        object.__setattr__(payload, "total_fee_usdt", Decimal("999"))
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="contradictory",
        ):
            canonical_prospective_position_terminal_payload_v2(payload)
    finally:
        _close(sources)


def test_reference_builders_reject_binary_float_costs(tmp_path: Path) -> None:
    sources = _opened_sources(tmp_path, side=MandatoryExitPositionSideV2.SHORT)
    try:
        with pytest.raises(
            ProspectivePositionTerminalContractErrorV2,
            match="finite Decimal",
        ):
            build_final_fee_evidence_reference_v2(
                attempt_id=sources.plan.attempt_id,
                promoting_plan_sha256=sources.plan.promoting_plan_sha256,
                symbol=sources.cell.symbol,
                position_id=sources.family.position_id,
                mandatory_exit_fee_certificate_sha256="6" * 64,
                final_timeline_checkpoint_sha256="9" * 64,
                final_timeline_root_sha256="b" * 64,
                fee_position_payload_sha256="c" * 64,
                exit_slices_root_sha256="8" * 64,
                exit_slice_count=1,
                multiplier=FeeMultiplierV2.PRIMARY_1_0X,
                entry_fee_usdt=0.05,  # type: ignore[arg-type]
                exit_fee_usdt=Decimal("0.04"),
                total_fee_usdt=Decimal("0.09"),
            )
    finally:
        _close(sources)
