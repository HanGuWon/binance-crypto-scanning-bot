from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateQualificationV2,
    WalQualificationRunV2,
    select_wal_candidate_v2,
)
from signalbot.r4b_v2.execution.fees import FeeMultiplierV2
from signalbot.r4b_v2.execution.mandatory_exit import MandatoryExitPositionSideV2
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    size_fixed_quote_paper_entry_v2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_store import (
    ProspectiveOutcomeWalStoreConfigV2,
    ProspectiveOutcomeWalStoreFactoryV2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    build_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_position_lifecycle_owner import (
    ProspectiveFamilyExitDispositionPayloadV2,
    ProspectiveFamilyExitPreparePayloadV2,
    ProspectivePositionCashflowClassV2,
    ProspectivePositionLifecycleContractErrorV2,
    ProspectivePositionLifecycleIntegrityErrorV2,
    ProspectivePositionLifecycleOwnerFactoryV2,
    ProspectivePositionOpenDispositionPayloadV2,
    ProspectivePositionOpenDispositionV2,
    ProspectivePositionOpenIntentV2,
    ProspectivePositionOpenPreparePayloadV2,
    ProspectiveTypedLifecyclePhaseV2,
    build_prospective_position_open_disposition_payload_v2,
    build_prospective_position_open_prepare_payload_v2,
    canonical_prospective_position_open_prepare_payload_v2,
    parse_prospective_position_open_prepare_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_position_terminal_payload import (
    ProspectivePositionTerminalStatusV2,
    build_family_a_lifecycle_evidence_reference_v2,
    build_final_fee_evidence_reference_v2,
    build_funding_census_evidence_reference_v2,
    build_mandatory_exit_evidence_reference_v2,
    build_prospective_position_terminal_payload_v2,
    prospective_position_id_v2,
)
from signalbot.r4b_v2.protocol.lifecycle import MILLISECONDS_PER_DAY_V2
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAEntryDecisionV2,
    FamilyAEntryInputV2,
    FamilyAEpisodeLedgerV2,
)

from ..strategy import test_family_a as family_a_testkit
from ..strategy import test_family_b_lifecycle_receipts as family_b_receipt_testkit
from ..strategy import test_family_c_lifecycle_receipts as family_c_receipt_testkit
from . import test_prospective_outcome_wal_store as outcome_store_testkit
from . import test_prospective_paper_terminal_payload as paper_terminal_testkit
from . import test_prospective_position_terminal_payload as position_terminal_testkit


def _selection_receipt(plan):
    h_start_ms = plan.attempt.horizon.h_start_ms
    window_end_ms = h_start_ms - MILLISECONDS_PER_DAY_V2
    window_start_ms = window_end_ms - WAL_QUALIFICATION_DURATION_MS_V2
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=outcome_store_testkit._policy(  # pyright: ignore[reportPrivateUsage]
                sync_ms,
                record_cap,
            ),
            metrics=outcome_store_testkit._metrics(  # pyright: ignore[reportPrivateUsage]
                passed=(sync_ms, record_cap) == selected
            ),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}:{h_start_ms}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=outcome_store_testkit.QUALIFICATION_ID,
        window_start_wall_ms=window_start_ms,
        window_end_wall_ms=window_end_ms,
        actual_final_panel_sha256="1" * 64,
        final_codec_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
        independent_failure_domain_evidence_sha256="5" * 64,
        actual_final_panel_passed=True,
        final_codec_passed=True,
        independent_failure_domains_passed=True,
        engineering_only=True,
        strategy_or_outcome_data_accessed=False,
        candidates=candidates,
    )
    return select_wal_candidate_v2(
        qualification,
        selection_wall_ms=window_end_ms,
        h_start_wall_ms=h_start_ms,
    )


def _outcome_factory(plan, scope: Path) -> ProspectiveOutcomeWalStoreFactoryV2:
    primary = scope / "position-outcome-primary"
    mirror = scope / "position-outcome-mirror"
    primary.mkdir()
    mirror.mkdir()
    receipt = _selection_receipt(plan)
    policy = receipt.selected_policy
    assert policy is not None
    return ProspectiveOutcomeWalStoreFactoryV2(
        config=ProspectiveOutcomeWalStoreConfigV2(
            attempt_plan_sha256=plan.plan_sha256,
            primary_directory=primary,
            mirror_directory=mirror,
            policy=policy,
            selection_receipt=receipt,
            protocol_sha256=plan.execution_contract_sha256,
            source_manifest_sha256=plan.strategy_code_freeze_manifest_sha256,
            schema_sha256="d" * 64,
            runtime_manifest_sha256=receipt.qualification.runtime_manifest_sha256,
            primary_maximum_total_bytes=16 * 1_024 * 1_024,
            mirror_maximum_total_bytes=16 * 1_024 * 1_024,
            primary_emergency_reserve_bytes=1_024,
            mirror_emergency_reserve_bytes=1_024,
            primary_failure_domain_id="position-outcome-primary-device",
            mirror_failure_domain_id="position-outcome-mirror-device",
            maximum_batch_records=16,
            maximum_records=100,
            maximum_active_outcomes=10,
        )
    )


def _no_position_sources(tmp_path: Path):
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=Decimal("1.49")
        )
    )
    sources = paper_terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        item,
    )
    plan, cell, transaction, _daily_store, _lease = sources
    paper_terminal = build_prospective_paper_terminal_payload_v2(
        plan=plan,
        cell=cell,
        transaction=transaction,
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
    )
    return (*sources, paper_terminal)


def _family_a_full_fill_sources(tmp_path: Path):
    item: FamilyAEntryInputV2 = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan, cell, transaction, daily_store, lease = paper_terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
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
    return (
        plan,
        cell,
        daily_store,
        lease,
        item,
        paper_decision,
        certificate,
        paper_registry,
        paper_terminal,
        ledger,
        exact_signal,
    )


def test_no_position_owner_is_durable_restartable_and_terminal_once(
    tmp_path: Path,
) -> None:
    plan, cell, _transaction, daily_store, lease, paper_terminal = _no_position_sources(tmp_path)
    outcome_factory = _outcome_factory(plan, lease.scope_root)
    store = outcome_factory.open(census_plan=plan, writer_lease=lease)
    lifecycle_factory = ProspectivePositionLifecycleOwnerFactoryV2()
    owner = lifecycle_factory.open_fresh_v2(
        plan=plan,
        writer_lease=lease,
        outcome_store=store,
    )
    try:
        prepare_receipt = owner.prepare_position_open_v2(
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
        )
        prepare = prepare_receipt.payload
        assert type(prepare) is ProspectivePositionOpenPreparePayloadV2
        assert prepare.open_intent is ProspectivePositionOpenIntentV2.NO_POSITION
        disposition_receipt = owner.disposition_position_open_v2(
            prepare=prepare,
            dispositioned_at_ms=cell.decision_cutoff_ms,
        )
        disposition = disposition_receipt.payload
        assert type(disposition) is ProspectivePositionOpenDispositionPayloadV2
        assert disposition.disposition is (
            ProspectivePositionOpenDispositionV2.SUPPRESSED_NO_POSITION
        )
        terminal = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=cell.decision_cutoff_ms,
            paper_terminal_record_sha256="a" * 64,
        )
        assert terminal.terminal_status is (
            ProspectivePositionTerminalStatusV2.SUPPRESSED_NO_POSITION
        )
        owner.finalize_position_v2(terminal)
        typed_snapshot = owner.snapshot_v2()
        assert store.record_count == 3
        assert typed_snapshot.outcomes[0].phase is ProspectiveTypedLifecyclePhaseV2.TERMINAL
        store.close()

        replay = outcome_factory.verify_replay_snapshot_v2(
            census_plan=plan,
            writer_lease=lease,
        )
        recovered = lifecycle_factory.prepare_recovery_v2(
            plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
        )
        reopened = outcome_factory.open(
            census_plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
            recovered_state_owner=recovered,
        )
        recovered.attach_recovered_store_v2(reopened)
        assert recovered.snapshot_v2().outcomes[0].phase is (
            ProspectiveTypedLifecyclePhaseV2.TERMINAL
        )
        before = reopened.record_count
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match=r"terminal identity|terminal",
        ):
            recovered.finalize_position_v2(terminal)
        assert reopened.record_count == before
        reopened.abort()
    finally:
        if not store.record_count:
            store.abort()
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_no_position_terminal_time_is_monotone_with_exact_boundary(
    tmp_path: Path,
) -> None:
    plan, cell, _transaction, daily_store, lease, paper_terminal = _no_position_sources(tmp_path)
    outcome_factory = _outcome_factory(plan, lease.scope_root)
    store = outcome_factory.open(census_plan=plan, writer_lease=lease)
    owner = ProspectivePositionLifecycleOwnerFactoryV2().open_fresh_v2(
        plan=plan,
        writer_lease=lease,
        outcome_store=store,
    )
    dispositioned_at_ms = cell.decision_cutoff_ms + 10_000
    try:
        prepare = owner.prepare_position_open_v2(
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
        ).payload
        assert type(prepare) is ProspectivePositionOpenPreparePayloadV2
        owner.disposition_position_open_v2(
            prepare=prepare,
            dispositioned_at_ms=dispositioned_at_ms,
        )
        early = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=dispositioned_at_ms - 1,
            paper_terminal_record_sha256="a" * 64,
        )
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="predates the preceding durable lifecycle transition",
        ):
            owner.finalize_position_v2(early)
        assert store.record_count == 2

        boundary = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=dispositioned_at_ms,
            paper_terminal_record_sha256="a" * 64,
        )
        owner.finalize_position_v2(boundary)
        assert store.record_count == 3
    finally:
        store.abort()
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_payload_is_factory_sealed_strict_and_bounded(tmp_path: Path) -> None:
    plan, cell, _transaction, daily_store, lease, paper_terminal = _no_position_sources(tmp_path)
    try:
        payload = build_prospective_position_open_prepare_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
        )
        encoded = canonical_prospective_position_open_prepare_payload_v2(payload)
        assert parse_prospective_position_open_prepare_payload_v2(encoded) == payload
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="factory-sealed",
        ):
            replace(payload)

        document = json.loads(encoded)
        document["unknown"] = True
        with pytest.raises(ProspectivePositionLifecycleIntegrityErrorV2):
            parse_prospective_position_open_prepare_payload_v2(canonical_json_line(document))
        with pytest.raises(
            ProspectivePositionLifecycleIntegrityErrorV2,
            match="bound",
        ):
            parse_prospective_position_open_prepare_payload_v2(b"{" + b"x" * 65_536)
    finally:
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_full_fill_cannot_be_dispositioned_without_family_admission(
    tmp_path: Path,
) -> None:
    sources = position_terminal_testkit._opened_sources(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        side=position_terminal_testkit.MandatoryExitPositionSideV2.SHORT,
    )
    try:
        prepare = build_prospective_position_open_prepare_payload_v2(
            plan=sources.plan,
            cell=sources.cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=sources.paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=sources.cell.decision_cutoff_ms,
            full_fill_certificate=sources.certificate,
        )
        assert prepare.open_intent is ProspectivePositionOpenIntentV2.FULL_FILL_POSITION
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="family admission receipt",
        ):
            build_prospective_position_open_disposition_payload_v2(
                prepare=prepare,
                dispositioned_at_ms=sources.cell.decision_cutoff_ms,
            )
    finally:
        position_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            sources
        )


def test_recovery_flag_rejects_foreign_family_b_and_c_preexisting_receipts(
    tmp_path: Path,
) -> None:
    (
        plan,
        cell,
        daily_store,
        lease,
        _item,
        _paper_decision,
        certificate,
        _paper_registry,
        paper_terminal,
        _ledger,
        _exact_signal,
    ) = _family_a_full_fill_sources(tmp_path)
    try:
        prepare = build_prospective_position_open_prepare_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
            full_fill_certificate=certificate,
        )
        (
            b_item,
            b_decision,
            b_registry,
            b_paper,
            b_certificate,
            b_paper_registry,
        ) = family_b_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
        b_registry.admit_position_with_receipt(
            b_item,
            b_decision,
            paper_decision=b_paper,
            certificate=b_certificate,
            paper_registry=b_paper_registry,
        )
        b_preexisting = b_registry.admit_position_with_receipt(
            b_item,
            b_decision,
            paper_decision=b_paper,
            certificate=b_certificate,
            paper_registry=b_paper_registry,
        )
        (
            c_item,
            c_decision,
            c_ledger,
            c_paper,
            c_certificate,
            c_paper_registry,
        ) = family_c_receipt_testkit._admission_state()  # pyright: ignore[reportPrivateUsage]
        c_ledger.admit_position_with_receipt(
            c_item,
            c_decision,
            paper_decision=c_paper,
            certificate=c_certificate,
            paper_registry=c_paper_registry,
        )
        c_preexisting = c_ledger.admit_position_with_receipt(
            c_item,
            c_decision,
            paper_decision=c_paper,
            certificate=c_certificate,
            paper_registry=c_paper_registry,
        )
        assert b_preexisting.disposition.value == "PREEXISTING"
        assert c_preexisting.disposition.value == "PREEXISTING"
        for foreign_receipt in (b_preexisting, c_preexisting):
            with pytest.raises(
                ProspectivePositionLifecycleContractErrorV2,
                match="family differs",
            ):
                build_prospective_position_open_disposition_payload_v2(
                    prepare=prepare,
                    dispositioned_at_ms=cell.decision_cutoff_ms,
                    admission_receipt=foreign_receipt,
                    _allow_preexisting_recovery=True,
                )
    finally:
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_opened_incomplete_terminal_drains_pending_exit_and_restarts(
    tmp_path: Path,
) -> None:
    (
        plan,
        cell,
        daily_store,
        lease,
        item,
        paper_decision,
        certificate,
        paper_registry,
        paper_terminal,
        ledger,
        exact_signal,
    ) = _family_a_full_fill_sources(tmp_path)
    outcome_factory = _outcome_factory(plan, lease.scope_root)
    lifecycle_factory = ProspectivePositionLifecycleOwnerFactoryV2()
    store = outcome_factory.open(census_plan=plan, writer_lease=lease)
    owner = lifecycle_factory.open_fresh_v2(
        plan=plan,
        writer_lease=lease,
        outcome_store=store,
    )
    reopened = None
    try:
        prepare = owner.prepare_position_open_v2(
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
            full_fill_certificate=certificate,
        ).payload
        assert type(prepare) is ProspectivePositionOpenPreparePayloadV2
        premature = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=cell.decision_cutoff_ms,
            paper_terminal_record_sha256="a" * 64,
            full_fill_certificate=certificate,
        )
        assert premature.terminal_status is ProspectivePositionTerminalStatusV2.INCOMPLETE
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="admitted position lifecycle",
        ):
            owner.finalize_position_v2(premature)
        assert store.record_count == 1

        admission = ledger.admit_external_full_fill_with_receipt(
            item,
            exact_signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        open_disposition = owner.disposition_position_open_v2(
            prepare=prepare,
            dispositioned_at_ms=cell.decision_cutoff_ms,
            admission_receipt=admission,
        ).payload
        assert type(open_disposition) is ProspectivePositionOpenDispositionPayloadV2
        exit_input = family_a_testkit._exit_input_for_admission(  # pyright: ignore[reportPrivateUsage]
            admission,
            close_price=Decimal("111"),
        )
        exit_receipt = ledger.evaluate_exit_with_receipt(exit_input)
        assert exit_receipt.decision.exits_position
        owner.prepare_family_exit_v2(
            open_disposition=open_disposition,
            exit_decision=exit_receipt.decision,
            exit_input_sha256=exit_receipt.input_sha256,
            prepared_at_ms=exit_receipt.decision.decision_cutoff_ms,
        )
        incomplete = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=exit_receipt.decision.decision_cutoff_ms,
            paper_terminal_record_sha256="a" * 64,
            full_fill_certificate=certificate,
        )
        assert incomplete.terminal_status is ProspectivePositionTerminalStatusV2.INCOMPLETE
        owner.finalize_position_v2(incomplete)
        assert owner.active_outcome_count == 0
        assert store.active_outcome_count == 0
        assert store.record_count == 4
        store.close()

        replay = outcome_factory.verify_replay_snapshot_v2(
            census_plan=plan,
            writer_lease=lease,
        )
        recovered = lifecycle_factory.prepare_recovery_v2(
            plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
        )
        reopened = outcome_factory.open(
            census_plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
            recovered_state_owner=recovered,
        )
        recovered.attach_recovered_store_v2(reopened)
        assert recovered.active_outcome_count == 0
        assert recovered.snapshot_v2().outcomes[0].phase is (
            ProspectiveTypedLifecyclePhaseV2.TERMINAL
        )
    finally:
        if reopened is not None:
            reopened.abort()
        else:
            store.abort()
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_recovered_pending_a_admission_and_exit_accept_only_exact_preexisting(
    tmp_path: Path,
) -> None:
    (
        plan,
        cell,
        daily_store,
        lease,
        item,
        paper_decision,
        certificate,
        paper_registry,
        paper_terminal,
        ledger,
        exact_signal,
    ) = _family_a_full_fill_sources(tmp_path)
    outcome_factory = _outcome_factory(plan, lease.scope_root)
    lifecycle_factory = ProspectivePositionLifecycleOwnerFactoryV2()
    current_store = outcome_factory.open(census_plan=plan, writer_lease=lease)
    owner = lifecycle_factory.open_fresh_v2(
        plan=plan,
        writer_lease=lease,
        outcome_store=current_store,
    )
    try:
        prepare = owner.prepare_position_open_v2(
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
            full_fill_certificate=certificate,
        ).payload
        assert type(prepare) is ProspectivePositionOpenPreparePayloadV2
        admission_new = ledger.admit_external_full_fill_with_receipt(
            item,
            exact_signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        assert admission_new.disposition.value == "NEW_BY_THIS_TRANSACTION"
        admission_state = ledger.export_state_v2()
        admission_count = ledger.event_count
        admission_root = ledger.root_sha256
        current_store.close()

        replay = outcome_factory.verify_replay_snapshot_v2(
            census_plan=plan,
            writer_lease=lease,
        )
        recovered = lifecycle_factory.prepare_recovery_v2(
            plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
        )
        current_store = outcome_factory.open(
            census_plan=plan,
            writer_lease=lease,
            replay_snapshot=replay,
            recovered_state_owner=recovered,
        )
        recovered.attach_recovered_store_v2(current_store)
        restored = FamilyAEpisodeLedgerV2.restore_state_v2(
            admission_state,
            maximum_events=20,
            expected_event_count=admission_count,
            expected_root_sha256=admission_root,
        )
        admission_recovered = restored.admit_external_full_fill_with_receipt(
            item,
            exact_signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        assert admission_recovered.disposition.value == "PREEXISTING"
        assert admission_recovered.pre_root_sha256 == admission_root
        assert admission_recovered.post_root_sha256 == admission_root
        corrupted_recovery = restored.admit_external_full_fill_with_receipt(
            item,
            exact_signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        object.__setattr__(corrupted_recovery, "post_root_sha256", "f" * 64)
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="preserve the exact current state",
        ):
            recovered.disposition_position_open_v2(
                prepare=prepare,
                dispositioned_at_ms=cell.decision_cutoff_ms,
                admission_receipt=corrupted_recovery,
            )
        assert current_store.record_count == 1
        open_disposition = recovered.disposition_position_open_v2(
            prepare=prepare,
            dispositioned_at_ms=cell.decision_cutoff_ms,
            admission_receipt=admission_recovered,
        ).payload
        assert type(open_disposition) is ProspectivePositionOpenDispositionPayloadV2
        assert open_disposition.admission_evidence is not None
        assert open_disposition.admission_evidence.admission_disposition == "PREEXISTING"
        before_duplicate = current_store.record_count
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="only for recovered pending reconciliation",
        ):
            recovered.disposition_position_open_v2(
                prepare=prepare,
                dispositioned_at_ms=cell.decision_cutoff_ms,
                admission_receipt=admission_recovered,
            )
        assert current_store.record_count == before_duplicate

        exit_input = family_a_testkit._exit_input_for_admission(  # pyright: ignore[reportPrivateUsage]
            admission_recovered,
            close_price=Decimal("111"),
        )
        exit_new = restored.evaluate_exit_with_receipt(exit_input)
        assert exit_new.disposition.value == "NEW_BY_THIS_TRANSACTION"
        assert exit_new.decision.exits_position
        exit_prepare = recovered.prepare_family_exit_v2(
            open_disposition=open_disposition,
            exit_decision=exit_new.decision,
            exit_input_sha256=exit_new.input_sha256,
            prepared_at_ms=exit_new.decision.decision_cutoff_ms,
        ).payload
        assert type(exit_prepare) is ProspectiveFamilyExitPreparePayloadV2
        exit_state = restored.export_state_v2()
        exit_count = restored.event_count
        exit_root = restored.root_sha256
        current_store.close()

        exit_replay = outcome_factory.verify_replay_snapshot_v2(
            census_plan=plan,
            writer_lease=lease,
        )
        exit_owner = lifecycle_factory.prepare_recovery_v2(
            plan=plan,
            writer_lease=lease,
            replay_snapshot=exit_replay,
        )
        current_store = outcome_factory.open(
            census_plan=plan,
            writer_lease=lease,
            replay_snapshot=exit_replay,
            recovered_state_owner=exit_owner,
        )
        exit_owner.attach_recovered_store_v2(current_store)
        exit_restored = FamilyAEpisodeLedgerV2.restore_state_v2(
            exit_state,
            maximum_events=20,
            expected_event_count=exit_count,
            expected_root_sha256=exit_root,
        )
        exit_recovered = exit_restored.evaluate_exit_with_receipt(exit_input)
        assert exit_recovered.disposition.value == "PREEXISTING"
        assert exit_recovered.pre_root_sha256 == exit_root
        assert exit_recovered.post_root_sha256 == exit_root
        exit_disposition = exit_owner.disposition_family_exit_v2(
            prepare=exit_prepare,
            exit_receipt=exit_recovered,
            dispositioned_at_ms=exit_recovered.decision.decision_cutoff_ms,
        ).payload
        assert type(exit_disposition) is ProspectiveFamilyExitDispositionPayloadV2
        assert exit_disposition.exit_evidence.exit_disposition == "PREEXISTING"
        assert exit_owner.snapshot_v2().outcomes[0].phase is (
            ProspectiveTypedLifecyclePhaseV2.EXIT_TERMINAL
        )
        assert not exit_owner.typed_payload_semantics_authoritative
        assert not exit_owner.efficacy_eligible
        assert not exit_owner.production_order_placement
    finally:
        current_store.abort()
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )


def test_complete_position_reconciles_all_six_record_kinds(tmp_path: Path) -> None:
    item: FamilyAEntryInputV2 = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan, cell, transaction, daily_store, lease = paper_terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
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
    outcome_factory = _outcome_factory(plan, lease.scope_root)
    store = outcome_factory.open(census_plan=plan, writer_lease=lease)
    owner = ProspectivePositionLifecycleOwnerFactoryV2().open_fresh_v2(
        plan=plan,
        writer_lease=lease,
        outcome_store=store,
    )
    try:
        prepare_receipt = owner.prepare_position_open_v2(
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            paper_terminal_record_sha256="a" * 64,
            prepared_at_ms=cell.decision_cutoff_ms,
            full_fill_certificate=certificate,
        )
        prepare = prepare_receipt.payload
        assert type(prepare) is ProspectivePositionOpenPreparePayloadV2
        admission = ledger.admit_external_full_fill_with_receipt(
            item,
            exact_signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        open_receipt = owner.disposition_position_open_v2(
            prepare=prepare,
            dispositioned_at_ms=cell.decision_cutoff_ms,
            admission_receipt=admission,
        )
        open_disposition = open_receipt.payload
        assert type(open_disposition) is ProspectivePositionOpenDispositionPayloadV2

        exit_input = family_a_testkit._exit_input_for_admission(  # pyright: ignore[reportPrivateUsage]
            admission,
            close_price=Decimal("111"),
        )
        terminal_exit_receipt = ledger.evaluate_exit_with_receipt(exit_input)
        assert terminal_exit_receipt.decision.exits_position
        exit_prepare_receipt = owner.prepare_family_exit_v2(
            open_disposition=open_disposition,
            exit_decision=terminal_exit_receipt.decision,
            exit_input_sha256=terminal_exit_receipt.input_sha256,
            prepared_at_ms=terminal_exit_receipt.decision.decision_cutoff_ms,
        )
        exit_prepare = exit_prepare_receipt.payload
        assert type(exit_prepare) is ProspectiveFamilyExitPreparePayloadV2
        exit_disposition_receipt = owner.disposition_family_exit_v2(
            prepare=exit_prepare,
            exit_receipt=terminal_exit_receipt,
            dispositioned_at_ms=terminal_exit_receipt.decision.decision_cutoff_ms,
        )
        exit_disposition = exit_disposition_receipt.payload
        assert type(exit_disposition) is ProspectiveFamilyExitDispositionPayloadV2

        position_id = prospective_position_id_v2(
            outcome_id=prepare.identity.outcome_id,
            certificate_sha256=certificate.certificate_sha256,
        )
        family_reference = build_family_a_lifecycle_evidence_reference_v2(
            position_id=position_id,
            admission=admission,
            terminal_exit=terminal_exit_receipt,
        )
        terminal_at_ms = terminal_exit_receipt.decision.decision_cutoff_ms + 10_000
        exit_notional = certificate.executable_notional - Decimal("5")
        mandatory = build_mandatory_exit_evidence_reference_v2(
            attempt_id=plan.attempt_id,
            promoting_plan_sha256=plan.promoting_plan_sha256,
            symbol=cell.symbol,
            position_id=position_id,
            family_exit_event_id=family_reference.exit_event_id,
            side=MandatoryExitPositionSideV2.SHORT,
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
            signed_exit_cashflow_usdt=-exit_notional,
            terminal_at_ms=terminal_at_ms,
        )
        fee = build_final_fee_evidence_reference_v2(
            attempt_id=plan.attempt_id,
            promoting_plan_sha256=plan.promoting_plan_sha256,
            symbol=cell.symbol,
            position_id=position_id,
            mandatory_exit_fee_certificate_sha256=(mandatory.fee_certificate_sha256),
            final_timeline_checkpoint_sha256="9" * 64,
            final_timeline_root_sha256="b" * 64,
            fee_position_payload_sha256="c" * 64,
            exit_slices_root_sha256=mandatory.exit_slices_root_sha256,
            exit_slice_count=mandatory.exit_slice_count,
            multiplier=FeeMultiplierV2.PRIMARY_1_0X,
            entry_fee_usdt=Decimal("0.050"),
            exit_fee_usdt=Decimal("0.045"),
            total_fee_usdt=Decimal("0.095"),
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
        with pytest.raises(
            ProspectivePositionLifecycleContractErrorV2,
            match="predates its typed evidence horizon",
        ):
            owner.append_position_cashflow_v2(
                terminal_exit=exit_disposition,
                cashflow_class=ProspectivePositionCashflowClassV2.EXIT_EXECUTION,
                evidence=mandatory,
                observed_at_ms=terminal_at_ms - 1,
            )
        assert store.record_count == 4
        for cashflow_class, evidence in (
            (ProspectivePositionCashflowClassV2.ENTRY_EXECUTION, certificate),
            (ProspectivePositionCashflowClassV2.EXIT_EXECUTION, mandatory),
            (ProspectivePositionCashflowClassV2.PUBLIC_FEE, fee),
            (ProspectivePositionCashflowClassV2.PUBLIC_FUNDING, funding),
        ):
            owner.append_position_cashflow_v2(
                terminal_exit=exit_disposition,
                cashflow_class=cashflow_class,
                evidence=evidence,
                observed_at_ms=terminal_at_ms,
            )
        terminal = build_prospective_position_terminal_payload_v2(
            plan=plan,
            cell=cell,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            paper_terminal=paper_terminal,
            finalized_at_ms=terminal_at_ms,
            paper_terminal_record_sha256="a" * 64,
            full_fill_certificate=certificate,
            family_evidence=family_reference,
            mandatory_exit_evidence=mandatory,
            fee_evidence=fee,
            funding_evidence=funding,
        )
        owner.finalize_position_v2(terminal)
        assert store.record_count == 9
        assert owner.snapshot_v2().outcomes[0].cashflow_classes == tuple(
            sorted(ProspectivePositionCashflowClassV2, key=str)
        )
    finally:
        store.abort()
        paper_terminal_testkit._close(  # pyright: ignore[reportPrivateUsage]
            daily_store,
            lease,
        )
