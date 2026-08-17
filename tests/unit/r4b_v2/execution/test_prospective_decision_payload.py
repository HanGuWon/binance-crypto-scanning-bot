from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveFamilyRuleBindingV2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    ProspectiveDecisionPayloadContractErrorV2,
    ProspectiveDispositionClassV2,
    build_prospective_cell_disposition_payload_from_receipts_v2,
    build_prospective_cell_disposition_payload_v2,
    build_prospective_decision_prepare_payload_v2,
    canonical_prospective_cell_disposition_payload_v2,
    canonical_prospective_decision_prepare_payload_v2,
    parse_prospective_cell_disposition_payload_v2,
    parse_prospective_decision_prepare_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
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
    FamilyAEpisodeLedgerV2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FAMILY_B_RULE_VERSION_V2,
    FamilyBDecisionRegistryV2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_RULE_VERSION_V2,
    FamilyCEpisodeLedgerV2,
)

from ..strategy import test_family_a as family_a_testkit
from ..strategy import test_family_b as family_b_testkit
from ..strategy import test_family_c as family_c_testkit


def _plan(
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    h_start_ms: int,
) -> ProspectiveCensusPlanV2:
    return ProspectiveCensusPlanV2(
        attempt_id=attempt_id,
        attempt=ProspectiveAttemptV2(
            attempt_index=1,
            qualification_start_ms=(h_start_ms - 30 * MILLISECONDS_PER_DAY_V2),
            horizon=FixedHorizonV2(h_start_ms=h_start_ms),
        ),
        promoting_plan_sha256=promoting_plan_sha256,
        symbols=(symbol,),
        context_symbols=tuple(sorted({symbol, *(f"P{index:02d}USDT" for index in range(20))})),
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
        paper_fok_rule_version="paper-fok-test-v2",
        execution_contract_sha256="b" * 64,
        efficacy_gate_contract_sha256="e" * 64,
        strategy_code_freeze_manifest_sha256="c" * 64,
        created_at_ms=h_start_ms - 1,
    )


def test_family_a_prepare_commit_disposition_and_wal_round_trip_exactly() -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    owner = FamilyAEpisodeLedgerV2(maximum_events=8)
    preview = owner.preview_entry(item)
    plan = _plan(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms,
    )

    prepare = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=cell,
        preview=preview,
    )
    assert prepare.disposition_class is ProspectiveDispositionClassV2.SIGNAL
    assert prepare.signal_side == "SHORT"
    prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)
    assert (
        parse_prospective_decision_prepare_payload_v2(
            prepare_jsonl,
            plan=plan,
            cell=cell,
        )
        == prepare
    )
    prepare_record = build_prospective_wal_record_v2(
        ingest_seq=1,
        kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
        attempt_plan_sha256=plan.plan_sha256,
        segment_id=cell.segment_id,
        cell_id=cell.cell_id,
        payload_schema=DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
        canonical_payload_jsonl=prepare_jsonl,
        previous_record_sha256=None,
    )

    owner.commit_entry_preview(item, preview)
    disposition = build_prospective_cell_disposition_payload_v2(
        prepare=prepare,
        prepare_record_sha256=prepare_record.record_sha256,
        decision_receipt=ReceiptTimestamp(
            received_at_ms=item.decision_cutoff_ms,
            received_monotonic_ns=9_007_199_254_740_992,
        ),
        family_state_root_after_sha256=owner.root_sha256,
        family_state_event_count_after=owner.event_count,
    )
    disposition_jsonl = canonical_prospective_cell_disposition_payload_v2(disposition)
    assert (
        parse_prospective_cell_disposition_payload_v2(
            disposition_jsonl,
            prepare=prepare,
            prepare_record_sha256=prepare_record.record_sha256,
        )
        == disposition
    )
    disposition_record = build_prospective_wal_record_v2(
        ingest_seq=2,
        kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
        attempt_plan_sha256=plan.plan_sha256,
        segment_id=cell.segment_id,
        cell_id=cell.cell_id,
        payload_schema=CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
        canonical_payload_jsonl=disposition_jsonl,
        previous_record_sha256=prepare_record.record_sha256,
    )
    disposition_record.verify_integrity()


def test_family_b_and_c_prepare_round_trip_constructor_validated_decisions() -> None:
    b_item = family_b_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    b_owner = FamilyBDecisionRegistryV2(maximum_events=8)
    b_preview = b_owner.preview_entry(b_item)
    b_plan = _plan(
        attempt_id=b_item.attempt_id,
        promoting_plan_sha256=b_item.promoting_plan_sha256,
        symbol=b_item.symbol,
        h_start_ms=b_item.bar_open_ms,
    )
    b_cell = b_plan.expected_cell(
        family=PromotingFamilyV2.B,
        symbol=b_item.symbol,
        bar_open_ms=b_item.bar_open_ms,
    )
    b_prepare = build_prospective_decision_prepare_payload_v2(
        plan=b_plan,
        cell=b_cell,
        preview=b_preview,
    )
    assert (
        parse_prospective_decision_prepare_payload_v2(
            canonical_prospective_decision_prepare_payload_v2(b_prepare),
            plan=b_plan,
            cell=b_cell,
        )
        == b_prepare
    )

    c_item = family_c_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    c_owner = FamilyCEpisodeLedgerV2(maximum_events=8)
    c_preview = c_owner.preview_entry(c_item)
    c_plan = _plan(
        attempt_id=c_item.attempt_id,
        promoting_plan_sha256=c_item.promoting_plan_sha256,
        symbol=c_item.target_symbol,
        h_start_ms=c_item.bar_open_ms,
    )
    c_cell = c_plan.expected_cell(
        family=PromotingFamilyV2.C,
        symbol=c_item.target_symbol,
        bar_open_ms=c_item.bar_open_ms,
    )
    c_prepare = build_prospective_decision_prepare_payload_v2(
        plan=c_plan,
        cell=c_cell,
        preview=c_preview,
    )
    assert c_prepare.family_state_root_before_sha256 == (
        c_preview.decision.episode_ledger_root_sha256
    )
    assert (
        parse_prospective_decision_prepare_payload_v2(
            canonical_prospective_decision_prepare_payload_v2(c_prepare),
            plan=c_plan,
            cell=c_cell,
        )
        == c_prepare
    )


def test_no_signal_and_data_failure_are_not_conflated() -> None:
    no_signal_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=family_a_testkit.Decimal("1.49")
        )
    )
    inconclusive_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            readiness=family_a_testkit.FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            reasons=("TEST_INCONCLUSIVE",),
        )
    )
    plan = _plan(
        attempt_id=no_signal_item.attempt_id,
        promoting_plan_sha256=no_signal_item.promoting_plan_sha256,
        symbol=no_signal_item.symbol,
        h_start_ms=no_signal_item.bar_open_ms,
    )
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=no_signal_item.symbol,
        bar_open_ms=no_signal_item.bar_open_ms,
    )
    no_signal = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=cell,
        preview=FamilyAEpisodeLedgerV2(maximum_events=2).preview_entry(no_signal_item),
    )
    inconclusive = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=cell,
        preview=FamilyAEpisodeLedgerV2(maximum_events=2).preview_entry(inconclusive_item),
    )
    assert no_signal.disposition_class is ProspectiveDispositionClassV2.NO_SIGNAL
    assert inconclusive.disposition_class is ProspectiveDispositionClassV2.INCONCLUSIVE
    assert no_signal.signal_side is inconclusive.signal_side is None

    active_owner = FamilyAEpisodeLedgerV2(maximum_events=4)
    active_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    active_signal = active_owner.evaluate_entry(active_item)
    paper_decision, certificate, paper_registry = family_a_testkit._paper_full_fill(  # pyright: ignore[reportPrivateUsage]
        active_signal
    )
    active_owner.admit_external_full_fill(
        active_item,
        active_signal,
        paper_decision,
        certificate,
        paper_registry,
    )
    next_open_ms = no_signal_item.bar_open_ms + 300_000
    next_close_ms = next_open_ms + 300_000 - 1
    next_cutoff_ms = next_close_ms + 2_001
    suppressed_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            bar_open_ms=next_open_ms,
            bar_close_ms=next_close_ms,
            decision_cutoff_ms=next_cutoff_ms,
            latest_source_event_ms=next_close_ms,
            latest_source_receipt_ms=next_cutoff_ms,
        )
    )
    suppressed = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=plan.expected_cell(
            family=PromotingFamilyV2.A,
            symbol=suppressed_item.symbol,
            bar_open_ms=suppressed_item.bar_open_ms,
        ),
        preview=active_owner.preview_entry(suppressed_item),
    )
    assert suppressed.disposition_class is ProspectiveDispositionClassV2.SUPPRESSED
    assert suppressed.signal_side is None


def test_payload_factories_reject_forgery_tamper_and_receipt_boundaries() -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    owner = FamilyAEpisodeLedgerV2(maximum_events=4)
    preview = owner.preview_entry(item)
    plan = _plan(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms,
    )
    prepare = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=cell,
        preview=preview,
    )
    encoded = canonical_prospective_decision_prepare_payload_v2(prepare)

    with pytest.raises(
        ProspectiveDecisionPayloadContractErrorV2,
        match="factory-sealed",
    ):
        replace(prepare)
    tampered = json.loads(encoded)
    tampered["family_status"] = "NO_SIGNAL"
    with pytest.raises(ProspectiveDecisionPayloadContractErrorV2):
        parse_prospective_decision_prepare_payload_v2(
            canonical_json_line(tampered),
            plan=plan,
            cell=cell,
        )
    unknown = json.loads(encoded)
    unknown["unknown"] = True
    with pytest.raises(
        ProspectiveDecisionPayloadContractErrorV2,
        match="schema",
    ):
        parse_prospective_decision_prepare_payload_v2(
            canonical_json_line(unknown),
            plan=plan,
            cell=cell,
        )

    prepare_record = build_prospective_wal_record_v2(
        ingest_seq=1,
        kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
        attempt_plan_sha256=plan.plan_sha256,
        segment_id=cell.segment_id,
        cell_id=cell.cell_id,
        payload_schema=DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
        canonical_payload_jsonl=encoded,
        previous_record_sha256=None,
    )
    owner.commit_entry_preview(item, preview)
    with pytest.raises(
        ProspectiveDecisionPayloadContractErrorV2,
        match="cannot precede",
    ):
        build_prospective_cell_disposition_payload_v2(
            prepare=prepare,
            prepare_record_sha256=prepare_record.record_sha256,
            decision_receipt=ReceiptTimestamp(
                received_at_ms=item.decision_cutoff_ms - 1,
                received_monotonic_ns=1,
            ),
            family_state_root_after_sha256=owner.root_sha256,
            family_state_event_count_after=owner.event_count,
        )
    with pytest.raises(
        ProspectiveDecisionPayloadContractErrorV2,
        match="change the owner root",
    ):
        build_prospective_cell_disposition_payload_v2(
            prepare=prepare,
            prepare_record_sha256=prepare_record.record_sha256,
            decision_receipt=ReceiptTimestamp(
                received_at_ms=item.decision_cutoff_ms,
                received_monotonic_ns=1,
            ),
            family_state_root_after_sha256=(prepare.family_state_root_before_sha256),
            family_state_event_count_after=owner.event_count,
        )


def test_committed_preview_cannot_mint_a_second_prepare() -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    owner = FamilyAEpisodeLedgerV2(maximum_events=4)
    owner.evaluate_entry(item)
    replay = owner.preview_entry(item)
    plan = _plan(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms,
    )
    with pytest.raises(
        ProspectiveDecisionPayloadContractErrorV2,
        match="uncommitted",
    ):
        build_prospective_decision_prepare_payload_v2(
            plan=plan,
            cell=cell,
            preview=replay,
        )


def test_disposition_factory_derives_state_and_record_only_from_sealed_receipts(
    tmp_path: Path,
) -> None:
    from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
        ProspectiveDailyWalStoreContractErrorV2,
        ProspectiveDailyWalStoreFactoryV2,
    )

    from . import test_prospective_daily_wal_store as store_testkit

    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    family_a = FamilyAEpisodeLedgerV2(maximum_events=4)
    preview = family_a.preview_entry(item)
    plan = _plan(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms,
    )
    prepare = build_prospective_decision_prepare_payload_v2(
        plan=plan,
        cell=cell,
        preview=preview,
    )
    prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)

    scope, primary, mirror = store_testkit._storage_paths(  # pyright: ignore[reportPrivateUsage]
        tmp_path
    )
    config = store_testkit._config(  # pyright: ignore[reportPrivateUsage]
        plan,
        primary,
        mirror,
    )
    config = replace(
        config,
        selection_receipt=replace(
            config.selection_receipt,
            h_start_wall_ms=plan.attempt.horizon.h_start_ms,
        ),
    )
    lease = WriterLease.acquire(scope)
    store = ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=store_testkit._ReceiptClock(  # pyright: ignore[reportPrivateUsage]
            wall_ms=plan.attempt.horizon.h_start_ms - 1
        ),
        clock_ns=store_testkit._ConstantClock(),  # pyright: ignore[reportPrivateUsage]
    ).open(census_plan=plan, writer_lease=lease)
    try:
        durable_prepare = store.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            canonical_payload_jsonl=prepare_jsonl,
        )
        commit_receipt = family_a.commit_entry_preview_with_receipt(item, preview)
        disposition = build_prospective_cell_disposition_payload_from_receipts_v2(
            prepare=prepare,
            prepare_durable_receipt=durable_prepare,
            commit_receipt=commit_receipt,
            decision_receipt=ReceiptTimestamp(
                received_at_ms=item.decision_cutoff_ms,
                received_monotonic_ns=9_007_199_254_740_992,
            ),
        )
        assert disposition.prepare_record_sha256 == (durable_prepare.records[0].record_sha256)
        assert disposition.family_state_root_after_sha256 == family_a.root_sha256
        assert disposition.family_state_event_count_after == family_a.event_count

        family_b_item = family_b_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
        family_b = FamilyBDecisionRegistryV2(maximum_events=4)
        family_b_preview = family_b.preview_entry(family_b_item)
        foreign_receipt = family_b.commit_entry_preview_with_receipt(
            family_b_item,
            family_b_preview,
        )
        with pytest.raises(
            ProspectiveDecisionPayloadContractErrorV2,
            match="differs from the durable PREPARE",
        ):
            build_prospective_cell_disposition_payload_from_receipts_v2(
                prepare=prepare,
                prepare_durable_receipt=durable_prepare,
                commit_receipt=foreign_receipt,
                decision_receipt=ReceiptTimestamp(
                    received_at_ms=item.decision_cutoff_ms,
                    received_monotonic_ns=1,
                ),
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="factory-sealed",
        ):
            replace(durable_prepare)
    finally:
        store.abort()
        lease.release()
