from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import ProspectiveCensusPlanV2
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    ProspectiveDailyWalStoreContractErrorV2,
    ProspectiveDailyWalStoreFactoryV2,
    ProspectiveDailyWalStoreV2,
)
from signalbot.r4b_v2.execution.prospective_decision_owner import (
    PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2,
    ProspectiveDecisionTransactionErrorV2,
    ProspectiveDecisionTransactionIndeterminateErrorV2,
    ProspectiveDecisionTransactionOwnerV2,
    ProspectiveDecisionTransactionStageV2,
    build_prospective_decision_transaction_owner_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    ProspectiveWalRecordKindV2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAContractError,
    FamilyAEpisodeLedgerV2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBContractError,
    FamilyBDecisionRegistryV2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCContractError,
    FamilyCEpisodeLedgerV2,
)

from ..strategy import test_family_a as family_a_testkit
from ..strategy import test_family_b as family_b_testkit
from ..strategy import test_family_c as family_c_testkit
from . import test_prospective_daily_wal_store as store_testkit
from . import test_prospective_decision_payload as payload_testkit


class _DecisionReceiptClock:
    def __init__(self, *, wall_ms: int, monotonic_ns: int = 123_456) -> None:
        self._wall_ms = wall_ms
        self._monotonic_ns = monotonic_ns
        self.capture_count = 0

    def capture(self) -> ReceiptTimestamp:
        self.capture_count += 1
        return ReceiptTimestamp(
            received_at_ms=self._wall_ms,
            received_monotonic_ns=self._monotonic_ns,
        )


class _RaiseAtStage:
    def __init__(self, target: ProspectiveDecisionTransactionStageV2) -> None:
        self._target = target
        self.seen: list[ProspectiveDecisionTransactionStageV2] = []

    def __call__(self, stage: ProspectiveDecisionTransactionStageV2) -> None:
        self.seen.append(stage)
        if stage is self._target:
            raise RuntimeError(f"fault at {stage.value}")


class _RaiseOnNthWalFsync:
    def __init__(self, *, occurrence: int) -> None:
        self._occurrence = occurrence
        self._count = 0

    def __call__(self, point: str) -> None:
        if point != "after_wal_fsync":
            return
        self._count += 1
        if self._count == self._occurrence:
            raise RuntimeError("ambiguous disposition fsync")


def _plan_for(
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    h_start_ms: int,
) -> ProspectiveCensusPlanV2:
    return payload_testkit._plan(  # pyright: ignore[reportPrivateUsage]
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
        symbol=symbol,
        h_start_ms=h_start_ms,
    )


def _open_store(
    tmp_path: Path,
    *,
    plan: ProspectiveCensusPlanV2,
    typed_decisions: bool = True,
    primary_fault_hook: Callable[[str], None] | None = None,
) -> tuple[WriterLease, ProspectiveDailyWalStoreV2]:
    scope, primary, mirror = store_testkit._storage_paths(  # pyright: ignore[reportPrivateUsage]
        tmp_path
    )
    config = store_testkit._config(  # pyright: ignore[reportPrivateUsage]
        plan,
        primary,
        mirror,
    )
    selection = replace(
        config.selection_receipt,
        h_start_wall_ms=plan.attempt.horizon.h_start_ms,
    )
    config = replace(
        config,
        selection_receipt=selection,
        typed_decision_payloads_required=typed_decisions,
    )
    lease = WriterLease.acquire(scope)
    receipt_clock = store_testkit._ReceiptClock(  # pyright: ignore[reportPrivateUsage]
        wall_ms=plan.attempt.horizon.h_start_ms - 1
    )
    store = ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=receipt_clock,
        clock_ns=store_testkit._ConstantClock(),  # pyright: ignore[reportPrivateUsage]
        primary_fault_hook=primary_fault_hook,
        recover_torn_tail=False,
    ).open(census_plan=plan, writer_lease=lease)
    return lease, store


def _build_owner(
    *,
    plan: ProspectiveCensusPlanV2,
    lease: WriterLease,
    store: ProspectiveDailyWalStoreV2,
    decision_receipt_ms: int,
    fault_hook: Callable[[ProspectiveDecisionTransactionStageV2], None] | None = None,
    family_a: FamilyAEpisodeLedgerV2 | None = None,
    family_b: FamilyBDecisionRegistryV2 | None = None,
    family_c: FamilyCEpisodeLedgerV2 | None = None,
) -> tuple[
    ProspectiveDecisionTransactionOwnerV2,
    FamilyAEpisodeLedgerV2,
    FamilyBDecisionRegistryV2,
    FamilyCEpisodeLedgerV2,
    _DecisionReceiptClock,
]:
    maximum_events = (
        plan.expected_bar_count
        * len(plan.symbols)
        * PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2
    )
    family_a = (
        FamilyAEpisodeLedgerV2(maximum_events=maximum_events) if family_a is None else family_a
    )
    family_b = (
        FamilyBDecisionRegistryV2(maximum_events=maximum_events) if family_b is None else family_b
    )
    family_c = (
        FamilyCEpisodeLedgerV2(maximum_events=maximum_events) if family_c is None else family_c
    )
    clock = _DecisionReceiptClock(wall_ms=decision_receipt_ms)
    owner = build_prospective_decision_transaction_owner_v2(
        census_plan=plan,
        writer_lease=lease,
        store=store,
        receipt_clock=clock,
        family_a=family_a,
        family_b=family_b,
        family_c=family_c,
        fault_hook=fault_hook,
    )
    return owner, family_a, family_b, family_c, clock


def _abort_and_release(
    store: ProspectiveDailyWalStoreV2,
    lease: WriterLease,
) -> None:
    store.abort()
    lease.release()


def test_family_a_transaction_is_adjacent_durable_and_not_replay_appended(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    lease, store = _open_store(tmp_path, plan=plan)
    owner, family_a, _, _, clock = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    try:
        result = owner.transact_family_a(cell=cell, item=item)

        assert result.decision == family_a.preview_entry(item).decision
        assert family_a.event_count == 1
        assert clock.capture_count == 1
        assert result.prepare_durable_receipt.records[0].ingest_seq == 1
        assert result.disposition_durable_receipt.records[0].ingest_seq == 2
        assert (
            result.prepare_durable_receipt.records[0].kind
            is ProspectiveWalRecordKindV2.DECISION_PREPARE
        )
        assert (
            result.disposition_durable_receipt.records[0].kind
            is ProspectiveWalRecordKindV2.CELL_DISPOSITION
        )
        assert not result.paper_fok_evaluated
        assert not result.production_order_placement

        with pytest.raises(ValueError, match="uncommitted"):
            owner.transact_family_a(cell=cell, item=item)
        assert family_a.event_count == 1
    finally:
        _abort_and_release(store, lease)


def test_family_b_and_c_transactions_use_their_exact_state_roots(
    tmp_path: Path,
) -> None:
    b_item = family_b_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    b_plan = _plan_for(
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
    b_lease, b_store = _open_store(tmp_path / "family-b", plan=b_plan)
    b_owner, _, family_b, _, _ = _build_owner(
        plan=b_plan,
        lease=b_lease,
        store=b_store,
        decision_receipt_ms=b_item.decision_cutoff_ms,
    )
    try:
        b_result = b_owner.transact_family_b(cell=b_cell, item=b_item)
        assert b_result.disposition_payload.family_state_root_after_sha256 == (
            family_b.replay_root_sha256
        )
        assert family_b.event_count == 1
    finally:
        _abort_and_release(b_store, b_lease)

    c_item = family_c_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    c_plan = _plan_for(
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
    c_lease, c_store = _open_store(tmp_path / "family-c", plan=c_plan)
    c_owner, _, _, family_c, _ = _build_owner(
        plan=c_plan,
        lease=c_lease,
        store=c_store,
        decision_receipt_ms=c_item.decision_cutoff_ms,
    )
    try:
        c_result = c_owner.transact_family_c(cell=c_cell, item=c_item)
        assert c_result.disposition_payload.family_state_root_after_sha256 == (family_c.root_sha256)
        assert family_c.event_count == 1
    finally:
        _abort_and_release(c_store, c_lease)


@pytest.mark.parametrize(
    "stage",
    (
        ProspectiveDecisionTransactionStageV2.AFTER_PREPARE_DURABLE,
        ProspectiveDecisionTransactionStageV2.AFTER_RECEIPT,
        ProspectiveDecisionTransactionStageV2.AFTER_STATE_COMMIT,
    ),
)
def test_pre_disposition_faults_leave_no_family_state_commit(
    tmp_path: Path,
    stage: ProspectiveDecisionTransactionStageV2,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    lease, store = _open_store(tmp_path, plan=plan)
    fault = _RaiseAtStage(stage)
    owner, family_a, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
        fault_hook=fault,
    )
    pre_root = family_a.root_sha256
    try:
        with pytest.raises(RuntimeError, match="fault at"):
            owner.transact_family_a(cell=cell, item=item)
        assert family_a.event_count == 0
        assert family_a.root_sha256 == pre_root
        with pytest.raises(Exception, match="PREPARE"):
            owner.transact_family_a(cell=cell, item=item)
    finally:
        _abort_and_release(store, lease)


def test_store_allows_only_one_decision_transaction_owner(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    lease, store = _open_store(tmp_path, plan=plan)
    _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match=r"different decision transaction owner|already has",
        ):
            _build_owner(
                plan=plan,
                lease=lease,
                store=store,
                decision_receipt_ms=item.decision_cutoff_ms,
            )
    finally:
        _abort_and_release(store, lease)


def test_prepopulated_family_rejected_before_store_claim_is_consumed(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    maximum_events = (
        plan.expected_bar_count
        * len(plan.symbols)
        * PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2
    )
    family_a = FamilyAEpisodeLedgerV2(maximum_events=maximum_events)
    prepopulated_b = FamilyBDecisionRegistryV2(maximum_events=maximum_events)
    family_c = FamilyCEpisodeLedgerV2(maximum_events=maximum_events)
    prepopulated_b.evaluate_entry(  # pyright: ignore[reportPrivateUsage]
        family_b_testkit._entry_input()
    )
    lease, store = _open_store(tmp_path, plan=plan)
    try:
        with pytest.raises(FamilyBContractError, match="requires exact genesis state"):
            _build_owner(
                plan=plan,
                lease=lease,
                store=store,
                decision_receipt_ms=item.decision_cutoff_ms,
                family_a=family_a,
                family_b=prepopulated_b,
                family_c=family_c,
            )

        fresh_b = FamilyBDecisionRegistryV2(maximum_events=maximum_events)
        owner, returned_a, returned_b, returned_c, _ = _build_owner(
            plan=plan,
            lease=lease,
            store=store,
            decision_receipt_ms=item.decision_cutoff_ms,
            family_a=family_a,
            family_b=fresh_b,
            family_c=family_c,
        )
        assert returned_a is family_a
        assert returned_b is fresh_b
        assert returned_c is family_c
        with pytest.raises(FamilyAContractError, match="held prospective"):
            returned_a.evaluate_entry(item)
        assert type(owner) is ProspectiveDecisionTransactionOwnerV2
    finally:
        _abort_and_release(store, lease)


def test_authority_blocks_concurrent_direct_family_mutations(tmp_path: Path) -> None:
    a_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    b_item = family_b_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    c_item = family_c_testkit._entry_input()  # pyright: ignore[reportPrivateUsage]
    plan = _plan_for(
        attempt_id=a_item.attempt_id,
        promoting_plan_sha256=a_item.promoting_plan_sha256,
        symbol=a_item.symbol,
        h_start_ms=a_item.bar_open_ms,
    )
    lease, store = _open_store(tmp_path, plan=plan)
    _, family_a, family_b, family_c, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=a_item.decision_cutoff_ms,
    )
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = (
                pool.submit(family_a.evaluate_entry, a_item),
                pool.submit(family_b.evaluate_entry, b_item),
                pool.submit(family_c.evaluate_entry, c_item),
            )
            with pytest.raises(FamilyAContractError, match="held prospective"):
                futures[0].result()
            with pytest.raises(FamilyBContractError, match="held prospective"):
                futures[1].result()
            with pytest.raises(FamilyCContractError, match="held prospective"):
                futures[2].result()
        assert (family_a.event_count, family_b.event_count, family_c.event_count) == (0, 0, 0)
    finally:
        _abort_and_release(store, lease)


def test_foreign_preexisting_commit_is_retained_and_poisons_owner(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    owner_ref: list[ProspectiveDecisionTransactionOwnerV2] = []
    ledger_ref: list[FamilyAEpisodeLedgerV2] = []

    def foreign_commit(stage: ProspectiveDecisionTransactionStageV2) -> None:
        if stage is not ProspectiveDecisionTransactionStageV2.AFTER_PREPARE_DURABLE:
            return
        ledger = ledger_ref[0]
        preview = ledger.preview_entry(item)
        ledger.commit_entry_preview_with_receipt(
            item,
            preview,
            _prospective_authority=owner_ref[0]._family_a_authority,  # pyright: ignore[reportPrivateUsage]
        )

    lease, store = _open_store(tmp_path, plan=plan)
    owner, family_a, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
        fault_hook=foreign_commit,
    )
    owner_ref.append(owner)
    ledger_ref.append(family_a)
    try:
        with pytest.raises(
            ProspectiveDecisionTransactionErrorV2,
            match="foreign concurrent actor",
        ):
            owner.transact_family_a(cell=cell, item=item)
        assert family_a.event_count == 1
        with pytest.raises(ProspectiveDecisionTransactionErrorV2, match="poisoned"):
            owner.transact_family_a(cell=cell, item=item)
    finally:
        _abort_and_release(store, lease)


def test_reverse_family_cell_fails_before_prepare_or_ledger_mutation(
    tmp_path: Path,
) -> None:
    base = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )

    def shifted_item(offset_ms: int, root_digit: str):
        bar_open_ms = base.bar_open_ms + offset_ms
        bar_close_ms = bar_open_ms + 300_000 - 1
        decision_cutoff_ms = bar_close_ms + (base.decision_cutoff_ms - base.bar_close_ms)
        return family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
            family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
                bar_open_ms=bar_open_ms,
                bar_close_ms=bar_close_ms,
                decision_cutoff_ms=decision_cutoff_ms,
                source_root_sha256=root_digit * 64,
                latest_source_event_ms=bar_close_ms,
                latest_source_receipt_ms=decision_cutoff_ms,
            )
        )

    later = shifted_item(300_000, "d")
    latest = shifted_item(600_000, "e")
    plan = _plan_for(
        attempt_id=base.attempt_id,
        promoting_plan_sha256=base.promoting_plan_sha256,
        symbol=base.symbol,
        h_start_ms=base.bar_open_ms,
    )
    lease, store = _open_store(tmp_path, plan=plan)
    owner, family_a, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=latest.decision_cutoff_ms,
    )
    later_cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=later.symbol,
        bar_open_ms=later.bar_open_ms,
    )
    base_cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=base.symbol,
        bar_open_ms=base.bar_open_ms,
    )
    latest_cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=latest.symbol,
        bar_open_ms=latest.bar_open_ms,
    )
    try:
        first = owner.transact_family_a(cell=later_cell, item=later)
        assert first.disposition_durable_receipt.records[0].ingest_seq == 2
        with pytest.raises(
            ProspectiveDecisionTransactionErrorV2,
            match="behind the committed family watermark",
        ):
            owner.transact_family_a(cell=base_cell, item=base)
        assert family_a.event_count == 1

        second = owner.transact_family_a(cell=latest_cell, item=latest)
        assert second.prepare_durable_receipt.records[0].ingest_seq == 3
        assert family_a.event_count == 2
    finally:
        _abort_and_release(store, lease)


def test_early_receipt_fails_before_state_commit(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    lease, store = _open_store(tmp_path, plan=plan)
    owner, family_a, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms - 1,
    )
    try:
        with pytest.raises(
            ProspectiveDecisionTransactionErrorV2,
            match="at/after cutoff",
        ):
            owner.transact_family_a(cell=cell, item=item)
        assert family_a.event_count == 0
    finally:
        _abort_and_release(store, lease)


def test_strict_store_rejects_schema_only_decision_json(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    lease, store = _open_store(tmp_path, plan=plan)
    owner, _, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="transaction-owner claim",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=store_testkit._payload(  # pyright: ignore[reportPrivateUsage]
                    ProspectiveWalRecordKindV2.DECISION_PREPARE
                ),
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="typed admission",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=store_testkit._payload(  # pyright: ignore[reportPrivateUsage]
                    ProspectiveWalRecordKindV2.DECISION_PREPARE
                ),
                transaction_claim=owner._transaction_claim,  # pyright: ignore[reportPrivateUsage]
            )
    finally:
        _abort_and_release(store, lease)


def test_decision_owner_rejects_non_typed_daily_store(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
        attempt_id=item.attempt_id,
        promoting_plan_sha256=item.promoting_plan_sha256,
        symbol=item.symbol,
        h_start_ms=item.bar_open_ms,
    )
    lease, store = _open_store(
        tmp_path,
        plan=plan,
        typed_decisions=False,
    )
    maximum_events = (
        plan.expected_bar_count
        * len(plan.symbols)
        * PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2
    )
    family_a = FamilyAEpisodeLedgerV2(maximum_events=maximum_events)
    family_b = FamilyBDecisionRegistryV2(maximum_events=maximum_events)
    family_c = FamilyCEpisodeLedgerV2(maximum_events=maximum_events)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="requires typed decision payload replay",
        ):
            _build_owner(
                plan=plan,
                lease=lease,
                store=store,
                decision_receipt_ms=item.decision_cutoff_ms,
                family_a=family_a,
                family_b=family_b,
                family_c=family_c,
            )
        assert family_a.evaluate_entry(item).event_id
        assert family_b.evaluate_entry(  # pyright: ignore[reportPrivateUsage]
            family_b_testkit._entry_input()
        ).event_id
        assert family_c.evaluate_entry(  # pyright: ignore[reportPrivateUsage]
            family_c_testkit._entry_input()
        ).event_id
    finally:
        _abort_and_release(store, lease)


def test_ambiguous_disposition_fsync_retains_state_and_fails_stop(
    tmp_path: Path,
) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    lease, store = _open_store(
        tmp_path,
        plan=plan,
        primary_fault_hook=_RaiseOnNthWalFsync(occurrence=2),
    )
    owner, family_a, _, _, _ = _build_owner(
        plan=plan,
        lease=lease,
        store=store,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    try:
        with pytest.raises(
            ProspectiveDecisionTransactionIndeterminateErrorV2,
            match="state is retained",
        ):
            owner.transact_family_a(cell=cell, item=item)
        assert family_a.event_count == 1
        with pytest.raises(Exception, match="poisoned"):
            owner.transact_family_a(cell=cell, item=item)
    finally:
        _abort_and_release(store, lease)


def test_reopen_strictly_replays_prior_typed_decision_records(tmp_path: Path) -> None:
    item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence()  # pyright: ignore[reportPrivateUsage]
    )
    plan = _plan_for(
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
    first_lease, first = _open_store(tmp_path, plan=plan)
    owner, _, _, _, _ = _build_owner(
        plan=plan,
        lease=first_lease,
        store=first,
        decision_receipt_ms=item.decision_cutoff_ms,
    )
    owner.transact_family_a(cell=cell, item=item)
    config = first._factory.config  # pyright: ignore[reportPrivateUsage]
    scope = first_lease.scope_root
    first.close()
    first_lease.release()

    second_lease = WriterLease.acquire(scope)
    second = ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=store_testkit._FailReceiptClock(),  # pyright: ignore[reportPrivateUsage]
        clock_ns=store_testkit._ConstantClock(),  # pyright: ignore[reportPrivateUsage]
        recover_torn_tail=False,
    ).open(census_plan=plan, writer_lease=second_lease)
    next_cell = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol=item.symbol,
        bar_open_ms=item.bar_open_ms + 300_000,
    )
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="state-recovery owner",
        ):
            _build_owner(
                plan=plan,
                lease=second_lease,
                store=second,
                decision_receipt_ms=item.decision_cutoff_ms,
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="PAPER_TERMINAL requires",
        ):
            second.append_and_sync(
                cell=next_cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=store_testkit._payload(  # pyright: ignore[reportPrivateUsage]
                    ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                    sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                ),
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            )
        assert second.active_shard_count == 1
    finally:
        _abort_and_release(second, second_lease)
