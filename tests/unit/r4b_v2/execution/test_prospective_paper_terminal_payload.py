from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_fok import (
    PAPER_FOK_RULE_VERSION_V2,
    CommonQuantityGridV2,
)
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    PaperSizingStatusV2,
    size_fixed_quote_paper_entry_v2,
)
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    ProspectiveFamilyRuleBindingV2,
)
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    ProspectiveDailyWalStoreV2,
)
from signalbot.r4b_v2.execution.prospective_decision_owner import (
    ProspectiveDecisionTransactionResultV2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    ProspectiveDispositionClassV2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    PROSPECTIVE_PAPER_TERMINAL_AUTHORITY_V2,
    ProspectivePaperTerminalCompletenessV2,
    ProspectivePaperTerminalContractErrorV2,
    ProspectivePaperTerminalCostStateV2,
    ProspectivePaperTerminalPayloadV2,
    ProspectivePaperTerminalStatusV2,
    build_prospective_paper_terminal_payload_v2,
    canonical_prospective_paper_terminal_payload_v2,
    parse_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2,
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveWalRecordKindV2,
    build_prospective_wal_record_v2,
)
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FAMILY_A_RULE_VERSION_V2,
    FamilyAEntryDecisionV2,
    FamilyAEntryInputV2,
)
from signalbot.r4b_v2.strategy.family_a_features import FamilyAFeatureReadinessV2
from signalbot.r4b_v2.strategy.family_b import FAMILY_B_RULE_VERSION_V2
from signalbot.r4b_v2.strategy.family_c import FAMILY_C_RULE_VERSION_V2
from signalbot.r4b_v2.strategy.prospective_plan import (
    current_prospective_execution_contract_sha256_v2,
)

from ..strategy import test_family_a as family_a_testkit
from . import test_prospective_decision_owner as owner_testkit


def _plan_for_signal() -> ProspectiveCensusPlanV2:
    h_start_ms = family_a_testkit.BAR_OPEN
    return ProspectiveCensusPlanV2(
        attempt_id=family_a_testkit.ATTEMPT,
        attempt=ProspectiveAttemptV2(
            attempt_index=1,
            qualification_start_ms=h_start_ms - 30 * MILLISECONDS_PER_DAY_V2,
            horizon=FixedHorizonV2(h_start_ms=h_start_ms),
        ),
        promoting_plan_sha256=family_a_testkit.PLAN,
        symbols=(family_a_testkit.SYMBOL,),
        context_symbols=tuple(
            sorted(
                {
                    family_a_testkit.SYMBOL,
                    *(f"Q{index:02d}USDT" for index in range(20)),
                }
            )
        ),
        family_rules=(
            ProspectiveFamilyRuleBindingV2(
                PromotingFamilyV2.A,
                FAMILY_A_RULE_VERSION_V2,
            ),
            ProspectiveFamilyRuleBindingV2(
                PromotingFamilyV2.B,
                FAMILY_B_RULE_VERSION_V2,
            ),
            ProspectiveFamilyRuleBindingV2(
                PromotingFamilyV2.C,
                FAMILY_C_RULE_VERSION_V2,
            ),
        ),
        paper_fok_rule_version=PAPER_FOK_RULE_VERSION_V2,
        execution_contract_sha256=current_prospective_execution_contract_sha256_v2(),
        efficacy_gate_contract_sha256="e" * 64,
        strategy_code_freeze_manifest_sha256="c" * 64,
        created_at_ms=h_start_ms - 1,
    )


def _grid(*, minimum_units: int = 1, maximum_units: int = 10_000) -> CommonQuantityGridV2:
    return CommonQuantityGridV2(
        scale=100,
        residue_units=0,
        modulus_units=1,
        minimum_units=minimum_units,
        maximum_units=maximum_units,
        first_legal_units=minimum_units,
    )


def _transact(
    tmp_path: Path,
    item: FamilyAEntryInputV2,
) -> tuple[
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    ProspectiveDecisionTransactionResultV2,
    ProspectiveDailyWalStoreV2,
    WriterLease,
]:
    plan = _plan_for_signal()
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms,
    )
    lease, store = owner_testkit._open_store(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        plan=plan,
    )
    owner, _, _, _, _ = owner_testkit._build_owner(  # pyright: ignore[reportPrivateUsage]
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    result = owner.transact_family_a(cell=cell, item=item)
    return plan, cell, result, store, lease


def _close(
    store: ProspectiveDailyWalStoreV2,
    lease: WriterLease,
) -> None:
    owner_testkit._abort_and_release(  # pyright: ignore[reportPrivateUsage]
        store,
        lease,
    )


def test_no_signal_terminal_round_trips_and_uses_existing_wal_schema(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=Decimal("1.49")
        )
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    try:
        payload = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        )
        encoded = canonical_prospective_paper_terminal_payload_v2(payload)
        assert payload.terminal_status is ProspectivePaperTerminalStatusV2.NO_SIGNAL
        assert payload.completeness is ProspectivePaperTerminalCompletenessV2.COMPLETE
        assert payload.cost_state is ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION
        assert payload.known_fee_cost_usdt == payload.known_funding_cost_usdt == Decimal(0)
        assert payload.authority_status == PROSPECTIVE_PAPER_TERMINAL_AUTHORITY_V2
        assert not payload.position_terminal
        assert not payload.position_pnl_computed
        assert not payload.production_order_placement
        assert payload.terminal_rule_plan_bound
        assert not payload.typed_wal_replay_authoritative
        assert not payload.efficacy_eligible
        assert (
            parse_prospective_paper_terminal_payload_v2(
                encoded,
                plan=plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            )
            == payload
        )
        document = json.loads(encoded)
        assert document["known_fee_cost_usdt"] == "0"
        assert document["reference_price"] is None

        record = build_prospective_wal_record_v2(
            ingest_seq=3,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            attempt_plan_sha256=plan.plan_sha256,
            segment_id=payload.segment_id,
            cell_id=payload.cell_id,
            payload_schema=PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=encoded,
            previous_record_sha256=(
                transaction.disposition_durable_receipt.records[0].record_sha256
            ),
        )
        record.verify_integrity()
    finally:
        _close(store, lease)


def test_inconclusive_decision_is_explicitly_incomplete(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            reasons=("TEST_SOURCE_INCOMPLETE",),
        )
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    try:
        payload = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
        )
        assert payload.terminal_status is ProspectivePaperTerminalStatusV2.INCOMPLETE_DECISION
        assert payload.completeness is ProspectivePaperTerminalCompletenessV2.INCOMPLETE
        assert payload.cost_state is ProspectivePaperTerminalCostStateV2.INCOMPLETE_EVIDENCE
        assert not payload.costs_complete
        assert payload.signed_slippage_vs_reference_usdt is None
        assert payload.known_fee_cost_usdt is None
    finally:
        _close(store, lease)


def test_nonready_sizing_is_explicitly_suppressed(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    sizing = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("20000"),
        reference_evidence_sha256="d" * 64,
        quantity_grid=_grid(minimum_units=1),
    )
    assert sizing.status is PaperSizingStatusV2.BELOW_MINIMUM
    try:
        payload = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            sizing=sizing,
        )
        assert payload.terminal_status is ProspectivePaperTerminalStatusV2.SUPPRESSED_SIZING
        assert payload.completeness is ProspectivePaperTerminalCompletenessV2.SUPPRESSED
        assert payload.cost_state is ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION
        assert payload.reference_price == Decimal("20000")
        assert payload.requested_quantity is None
        assert payload.canonical_paper_decision_jsonl is None
    finally:
        _close(store, lease)


def test_full_fill_binds_sizing_quotes_certificate_and_deferred_costs(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    assert type(transaction.decision) is FamilyAEntryDecisionV2
    paper_decision, certificate, _ = family_a_testkit._paper_full_fill(  # pyright: ignore[reportPrivateUsage]
        transaction.decision
    )
    sizing = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("50"),
        reference_evidence_sha256=paper_decision.evidence_sha256,
        quantity_grid=_grid(),
    )
    assert sizing.requested_quantity == paper_decision.requested_quantity == Decimal("2")
    try:
        payload = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            sizing=sizing,
            paper_decision=paper_decision,
            full_fill_certificate=certificate,
        )
        assert (
            payload.terminal_status is ProspectivePaperTerminalStatusV2.PAPER_EXECUTED_FULL_QUANTITY
        )
        assert (
            payload.cost_state
            is ProspectivePaperTerminalCostStateV2.ENTRY_SLIPPAGE_ONLY_POSITION_COSTS_DEFERRED
        )
        assert not payload.costs_complete
        assert payload.executable_vwap == paper_decision.executable_vwap
        assert payload.opposite_bbo == paper_decision.opposite_bbo
        assert payload.full_fill_certificate_sha256 == certificate.certificate_sha256
        assert payload.signed_slippage_vs_reference_usdt is not None
        assert payload.known_fee_cost_usdt is None
        assert payload.position_after_cost_pnl_usdt is None
        document = json.loads(canonical_prospective_paper_terminal_payload_v2(payload))
        assert (
            len(canonical_prospective_paper_terminal_payload_v2(payload))
            <= MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2
        )
        assert isinstance(document["executable_vwap"], str)
        assert isinstance(document["signed_slippage_vs_reference_usdt"], str)
        assert document["position_after_cost_pnl_usdt"] is None
        assert document["sizing_reference_membership_authoritative"] is False
    finally:
        _close(store, lease)


def test_factories_reject_forgery_tamper_wrong_sources_and_missing_certificate(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    assert type(transaction.decision) is FamilyAEntryDecisionV2
    sizing = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("50"),
        reference_evidence_sha256="d" * 64,
        quantity_grid=_grid(),
    )
    paper_decision, certificate, _ = family_a_testkit._paper_full_fill(  # pyright: ignore[reportPrivateUsage]
        transaction.decision
    )
    try:
        with pytest.raises(
            ProspectivePaperTerminalContractErrorV2,
            match="different sizing cell",
        ):
            build_prospective_paper_terminal_payload_v2(
                plan=plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
                sizing=sizing,
            )
        with pytest.raises(
            ProspectivePaperTerminalContractErrorV2,
            match="requires a registry-issued certificate",
        ):
            build_prospective_paper_terminal_payload_v2(
                plan=plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                sizing=sizing,
                paper_decision=paper_decision,
            )

        payload = build_prospective_paper_terminal_payload_v2(
            plan=plan,
            cell=cell,
            transaction=transaction,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            sizing=sizing,
            paper_decision=paper_decision,
            full_fill_certificate=certificate,
        )
        with pytest.raises(ProspectivePaperTerminalContractErrorV2, match="factory-sealed"):
            replace(payload)
        document = json.loads(canonical_prospective_paper_terminal_payload_v2(payload))
        document["costs_complete"] = True
        with pytest.raises(
            ProspectivePaperTerminalContractErrorV2,
            match="differs from its exact typed sources",
        ):
            parse_prospective_paper_terminal_payload_v2(
                canonical_json_line(document),
                plan=plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                sizing=sizing,
                paper_decision=paper_decision,
                full_fill_certificate=certificate,
            )
        document["unknown"] = True
        with pytest.raises(
            ProspectivePaperTerminalContractErrorV2,
            match="schema",
        ):
            parse_prospective_paper_terminal_payload_v2(
                canonical_json_line(document),
                plan=plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                sizing=sizing,
                paper_decision=paper_decision,
                full_fill_certificate=certificate,
            )
    finally:
        _close(store, lease)


def test_current_execution_contract_is_required(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=Decimal("1.49")
        )
    )
    plan, cell, transaction, store, lease = _transact(tmp_path, item)
    foreign_plan = replace(plan, execution_contract_sha256="f" * 64)
    try:
        with pytest.raises(
            ProspectivePaperTerminalContractErrorV2,
            match="current prospective execution contract",
        ):
            build_prospective_paper_terminal_payload_v2(
                plan=foreign_plan,
                cell=cell,
                transaction=transaction,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            )
    finally:
        _close(store, lease)


def test_direct_constructor_is_sealed() -> None:
    with pytest.raises(
        ProspectivePaperTerminalContractErrorV2,
        match="factory-sealed",
    ):
        ProspectivePaperTerminalPayloadV2(
            attempt_id="forged",
            attempt_plan_sha256="a" * 64,
            promoting_plan_sha256="b" * 64,
            execution_contract_sha256="c" * 64,
            segment_id="d" * 64,
            cell_id="e" * 64,
            family=PromotingFamilyV2.A,
            family_rule_version="forged",
            symbol="BTCUSDT",
            venue=family_a_testkit.VenueV2.USDM_FUTURES,
            bar_open_ms=0,
            bar_close_ms=299_999,
            decision_cutoff_ms=302_000,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            prepare_payload_sha256="1" * 64,
            prepare_wal_payload_sha256="2" * 64,
            prepare_record_sha256="3" * 64,
            disposition_payload_sha256="4" * 64,
            disposition_wal_payload_sha256="5" * 64,
            disposition_record_sha256="6" * 64,
            decision_event_id="7" * 64,
            decision_payload_sha256="8" * 64,
            disposition_class=ProspectiveDispositionClassV2.NO_SIGNAL,
            signal_side=None,
            terminal_status=ProspectivePaperTerminalStatusV2.NO_SIGNAL,
            completeness=ProspectivePaperTerminalCompletenessV2.COMPLETE,
            reasons=("FORGED",),
            invalidation="FORGED",
            sizing_status=None,
            sizing_rule_version=None,
            sizing_sha256=None,
            reference_evidence_sha256=None,
            target_quote_notional_usdt=None,
            reference_price=None,
            unrounded_quantity=None,
            requested_quantity=None,
            quote_notional_at_reference=None,
            paper_status=None,
            paper_rule_version=None,
            paper_decision_event_id=None,
            paper_decision_payload_sha256=None,
            paper_evidence_sha256=None,
            paper_inconclusive_cause=None,
            paper_closure_method=None,
            target_venue_ms=None,
            target_state_last_ingest_seq=None,
            certified_quantity=None,
            filled_quantity=None,
            opposite_bbo=None,
            paper_price_cap=None,
            market_take_bound_price=None,
            executable_vwap=None,
            executable_notional=None,
            full_fill_certificate_sha256=None,
            signed_slippage_vs_reference_usdt=Decimal(0),
            known_fee_cost_usdt=Decimal(0),
            known_funding_cost_usdt=Decimal(0),
            position_after_cost_pnl_usdt=None,
            cost_state=ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION,
            costs_complete=True,
            canonical_sizing_jsonl=None,
            canonical_paper_decision_jsonl=None,
            canonical_full_fill_certificate_jsonl=None,
            _factory_token=object(),
        )
