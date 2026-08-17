"""Transactional owner for prospective Family A/B/C entry decisions.

The owner serializes one exact decision cell through durable PREPARE, local
receipt capture, strategy-state commit, and durable DISPOSITION.  It does not
recover orphan PREPARE records, derive a venue-time causal target, evaluate a
PAPER fill, seal a daily segment, or place an order.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import Final, cast

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    ProspectiveDailyWalDurableBatchReceiptV2,
    ProspectiveDailyWalDurableRecordV2,
    ProspectiveDailyWalStoreV2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    FamilyEntryDecisionV2,
    FamilyEntryPreviewV2,
    ProspectiveCellDispositionPayloadV2,
    ProspectiveDecisionPreparePayloadV2,
    build_prospective_cell_disposition_payload_from_receipts_v2,
    build_prospective_decision_prepare_payload_v2,
    canonical_prospective_cell_disposition_payload_v2,
    canonical_prospective_decision_prepare_payload_v2,
    parse_prospective_cell_disposition_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    ProspectiveWalRecordKindV2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAEntryCommitDispositionV2,
    FamilyAEntryCommitReceiptV2,
    FamilyAEntryInputV2,
    FamilyAEntryPreviewV2,
    FamilyAEpisodeLedgerV2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBDecisionRegistryV2,
    FamilyBEntryCommitDispositionV2,
    FamilyBEntryCommitReceiptV2,
    FamilyBEntryInputV2,
    FamilyBEntryPreviewV2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCEntryCommitDispositionV2,
    FamilyCEntryCommitReceiptV2,
    FamilyCEntryInputV2,
    FamilyCEntryPreviewV2,
    FamilyCEpisodeLedgerV2,
)

PROSPECTIVE_DECISION_OWNER_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.5.0_PROSPECTIVE_DECISION_OWNER"
PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2: Final = 3

_OWNER_FACTORY_TOKEN: Final = object()
_RESULT_FACTORY_TOKEN: Final = object()
_MAX_CANONICAL_INTEGER_V2: Final = 9_007_199_254_740_991
_MAX_MONOTONIC_NS_V2: Final = 9_223_372_036_854_775_807

type FamilyEntryCommitReceiptV2 = (
    FamilyAEntryCommitReceiptV2 | FamilyBEntryCommitReceiptV2 | FamilyCEntryCommitReceiptV2
)


class ProspectiveDecisionTransactionErrorV2(RuntimeError):
    """Raised when a prospective decision transaction cannot finish exactly."""


class ProspectiveDecisionTransactionIndeterminateErrorV2(ProspectiveDecisionTransactionErrorV2):
    """Raised after a DISPOSITION append starts but durability is uncertain."""


class ProspectiveDecisionTransactionStageV2(StrEnum):
    """Bounded fault-injection seams of one decision transaction."""

    AFTER_PREPARE_DURABLE = "AFTER_PREPARE_DURABLE"
    AFTER_RECEIPT = "AFTER_RECEIPT"
    AFTER_STATE_COMMIT = "AFTER_STATE_COMMIT"
    AFTER_DISPOSITION_DURABLE = "AFTER_DISPOSITION_DURABLE"


type ProspectiveDecisionFaultHookV2 = Callable[
    [ProspectiveDecisionTransactionStageV2],
    None,
]


@dataclass(frozen=True, slots=True)
class ProspectiveDecisionTransactionResultV2:
    """Exact durable identities returned only after the final forced sync."""

    decision: FamilyEntryDecisionV2
    decision_receipt: ReceiptTimestamp
    prepare_payload: ProspectiveDecisionPreparePayloadV2 = field(repr=False)
    disposition_payload: ProspectiveCellDispositionPayloadV2 = field(repr=False)
    prepare_durable_receipt: ProspectiveDailyWalDurableBatchReceiptV2 = field(repr=False)
    disposition_durable_receipt: ProspectiveDailyWalDurableBatchReceiptV2 = field(repr=False)
    _factory_token: InitVar[object | None] = None
    rule_version: str = field(
        init=False,
        default=PROSPECTIVE_DECISION_OWNER_RULE_VERSION_V2,
    )
    paper_fok_evaluated: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision transaction results are factory-sealed"
            )
        if self.decision != self.prepare_payload.decision:
            raise ProspectiveDecisionTransactionErrorV2(
                "returned decision differs from the durable PREPARE"
            )
        if type(self.decision_receipt) is not ReceiptTimestamp:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision receipt must be an exact ReceiptTimestamp"
            )
        prepare_record = _assert_single_durable_record(
            self.prepare_durable_receipt,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            cell_id=self.prepare_payload.cell_id,
            canonical_payload_jsonl=(
                canonical_prospective_decision_prepare_payload_v2(self.prepare_payload)
            ),
        )
        disposition_record = _assert_single_durable_record(
            self.disposition_durable_receipt,
            kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
            cell_id=self.prepare_payload.cell_id,
            canonical_payload_jsonl=(
                canonical_prospective_cell_disposition_payload_v2(self.disposition_payload)
            ),
        )
        if (
            self.prepare_durable_receipt.attempt_plan_sha256,
            self.prepare_durable_receipt.segment_id,
            self.prepare_durable_receipt.shard_plan_sha256,
        ) != (
            self.prepare_payload.attempt_plan_sha256,
            self.prepare_payload.segment_id,
            self.disposition_durable_receipt.shard_plan_sha256,
        ) or (
            self.disposition_durable_receipt.attempt_plan_sha256,
            self.disposition_durable_receipt.segment_id,
        ) != (
            self.prepare_payload.attempt_plan_sha256,
            self.prepare_payload.segment_id,
        ):
            raise ProspectiveDecisionTransactionErrorV2(
                "durable decision receipts differ from the frozen cell shard"
            )
        if disposition_record.ingest_seq != prepare_record.ingest_seq + 1:
            raise ProspectiveDecisionTransactionErrorV2(
                "PREPARE and DISPOSITION are not adjacent in the guarded WAL"
            )
        parse_prospective_cell_disposition_payload_v2(
            canonical_prospective_cell_disposition_payload_v2(self.disposition_payload),
            prepare=self.prepare_payload,
            prepare_record_sha256=prepare_record.record_sha256,
        )
        if (
            self.decision_receipt.received_at_ms
            != self.disposition_payload.decision_receipt_wall_ms
            or self.decision_receipt.received_monotonic_ns
            != self.disposition_payload.decision_receipt_monotonic_ns
        ):
            raise ProspectiveDecisionTransactionErrorV2(
                "returned receipt differs from the durable DISPOSITION"
            )
        if self.paper_fok_evaluated or self.production_order_placement:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision transaction result cannot claim execution"
            )


class ProspectiveDecisionTransactionOwnerV2:
    """Serialize strategy-state mutation with one exact daily WAL owner."""

    __slots__ = (
        "_census_plan",
        "_failed",
        "_family_a",
        "_family_a_authority",
        "_family_b",
        "_family_b_authority",
        "_family_c",
        "_family_c_authority",
        "_family_cell_watermarks",
        "_fault_hook",
        "_lock",
        "_receipt_clock",
        "_store",
        "_transaction_active",
        "_transaction_claim",
        "_writer_lease",
    )

    def __init__(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        store: ProspectiveDailyWalStoreV2,
        receipt_clock: ReceiptClock,
        family_a: FamilyAEpisodeLedgerV2,
        family_a_authority: object,
        family_b: FamilyBDecisionRegistryV2,
        family_b_authority: object,
        family_c: FamilyCEpisodeLedgerV2,
        family_c_authority: object,
        fault_hook: ProspectiveDecisionFaultHookV2 | None,
        transaction_claim: object,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _OWNER_FACTORY_TOKEN:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision transaction owners are factory-sealed"
            )
        self._census_plan = census_plan
        self._writer_lease = writer_lease
        self._store = store
        self._transaction_claim = transaction_claim
        self._receipt_clock = receipt_clock
        self._family_a = family_a
        self._family_a_authority = family_a_authority
        self._family_b = family_b
        self._family_b_authority = family_b_authority
        self._family_c = family_c
        self._family_c_authority = family_c_authority
        self._family_cell_watermarks: dict[
            PromotingFamilyV2,
            tuple[int, int, str],
        ] = {}
        self._failed: BaseException | None = None
        self._fault_hook = fault_hook
        self._lock = threading.RLock()
        self._transaction_active = False

    def transact_family_a(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        item: FamilyAEntryInputV2,
    ) -> ProspectiveDecisionTransactionResultV2:
        """Evaluate and durably commit one frozen Family A cell."""

        if type(item) is not FamilyAEntryInputV2:
            raise ProspectiveDecisionTransactionErrorV2("item must be exact FamilyAEntryInputV2")
        with self._transaction_guard():
            self._assert_binding_guarded()
            self._assert_cell_watermark_guarded(cell, PromotingFamilyV2.A)
            preview = self._family_a.preview_entry(item)
            return self._transact_guarded(
                cell=cell,
                preview=preview,
                commit=lambda: self._family_a.commit_entry_preview_with_receipt(
                    item,
                    preview,
                    _prospective_authority=self._family_a_authority,
                ),
                rollback=lambda receipt: self._family_a.rollback_entry_preview(
                    item,
                    preview,
                    cast(FamilyAEntryCommitReceiptV2, receipt),
                    _prospective_authority=self._family_a_authority,
                ),
            )

    def transact_family_b(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        item: FamilyBEntryInputV2,
    ) -> ProspectiveDecisionTransactionResultV2:
        """Evaluate and durably commit one frozen Family B cell."""

        if type(item) is not FamilyBEntryInputV2:
            raise ProspectiveDecisionTransactionErrorV2("item must be exact FamilyBEntryInputV2")
        with self._transaction_guard():
            self._assert_binding_guarded()
            self._assert_cell_watermark_guarded(cell, PromotingFamilyV2.B)
            preview = self._family_b.preview_entry(item)
            return self._transact_guarded(
                cell=cell,
                preview=preview,
                commit=lambda: self._family_b.commit_entry_preview_with_receipt(
                    item,
                    preview,
                    _prospective_authority=self._family_b_authority,
                ),
                rollback=lambda receipt: self._family_b.rollback_entry_preview(
                    item,
                    preview,
                    cast(FamilyBEntryCommitReceiptV2, receipt),
                    _prospective_authority=self._family_b_authority,
                ),
            )

    def transact_family_c(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        item: FamilyCEntryInputV2,
    ) -> ProspectiveDecisionTransactionResultV2:
        """Evaluate and durably commit one frozen Family C cell."""

        if type(item) is not FamilyCEntryInputV2:
            raise ProspectiveDecisionTransactionErrorV2("item must be exact FamilyCEntryInputV2")
        with self._transaction_guard():
            self._assert_binding_guarded()
            self._assert_cell_watermark_guarded(cell, PromotingFamilyV2.C)
            preview = self._family_c.preview_entry(item)
            return self._transact_guarded(
                cell=cell,
                preview=preview,
                commit=lambda: self._family_c.commit_entry_preview_with_receipt(
                    item,
                    preview,
                    _prospective_authority=self._family_c_authority,
                ),
                rollback=lambda receipt: self._family_c.rollback_entry_preview(
                    item,
                    preview,
                    cast(FamilyCEntryCommitReceiptV2, receipt),
                    _prospective_authority=self._family_c_authority,
                ),
            )

    def _assert_binding_guarded(self) -> None:
        self._store.assert_decision_transaction_binding_v2(
            census_plan=self._census_plan,
            writer_lease=self._writer_lease,
            transaction_claim=self._transaction_claim,
        )

    def _assert_cell_watermark_guarded(
        self,
        cell: ProspectiveExpectedCellV2,
        family: PromotingFamilyV2,
    ) -> None:
        if type(cell) is not ProspectiveExpectedCellV2 or cell.family is not family:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision cell differs from the exact transaction family"
            )
        expected = self._census_plan.expected_cell(
            family=family,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms,
        )
        if cell != expected:
            raise ProspectiveDecisionTransactionErrorV2(
                "decision cell differs from the frozen prospective grid"
            )
        ordinal = (
            cell.bar_open_ms,
            self._census_plan.symbols.index(cell.symbol),
        )
        current = self._family_cell_watermarks.get(family)
        if current is None:
            return
        current_ordinal = current[:2]
        if ordinal < current_ordinal or (ordinal == current_ordinal and cell.cell_id != current[2]):
            raise ProspectiveDecisionTransactionErrorV2(
                "decision cell is at or behind the committed family watermark"
            )

    def _advance_cell_watermark_guarded(
        self,
        cell: ProspectiveExpectedCellV2,
    ) -> None:
        ordinal = (
            cell.bar_open_ms,
            self._census_plan.symbols.index(cell.symbol),
        )
        current = self._family_cell_watermarks.get(cell.family)
        if current is not None and ordinal <= current[:2]:
            raise ProspectiveDecisionTransactionErrorV2(
                "durable decision did not advance its family watermark"
            )
        self._family_cell_watermarks[cell.family] = (*ordinal, cell.cell_id)

    @contextmanager
    def _transaction_guard(self) -> Iterator[None]:
        with self._lock:
            if self._failed is not None:
                raise ProspectiveDecisionTransactionErrorV2(
                    "decision transaction owner is poisoned after an indeterminate failure"
                ) from self._failed
            if self._transaction_active:
                raise ProspectiveDecisionTransactionErrorV2(
                    "decision transaction owner rejects reentrant use"
                )
            self._transaction_active = True
            try:
                with self._writer_lease.operation_guard():
                    yield
            finally:
                self._transaction_active = False

    def _transact_guarded(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        preview: FamilyEntryPreviewV2,
        commit: Callable[[], FamilyEntryCommitReceiptV2],
        rollback: Callable[[FamilyEntryCommitReceiptV2], bool],
    ) -> ProspectiveDecisionTransactionResultV2:
        prepare = build_prospective_decision_prepare_payload_v2(
            plan=self._census_plan,
            cell=cell,
            preview=preview,
        )
        prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)
        prepare_receipt = self._store.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            canonical_payload_jsonl=prepare_jsonl,
            transaction_claim=self._transaction_claim,
        )
        _assert_single_durable_record(
            prepare_receipt,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            cell_id=prepare.cell_id,
            canonical_payload_jsonl=prepare_jsonl,
        )
        self._run_fault_hook(ProspectiveDecisionTransactionStageV2.AFTER_PREPARE_DURABLE)

        decision_receipt = self._receipt_clock.capture()
        _validate_receipt_before_commit(decision_receipt, prepare)
        self._run_fault_hook(ProspectiveDecisionTransactionStageV2.AFTER_RECEIPT)

        state_committed = False
        disposition_durable = False
        disposition_append_started = False
        commit_returned = False
        commit_receipt: FamilyEntryCommitReceiptV2 | None = None
        try:
            commit_receipt = commit()
            commit_returned = True
            state_committed = _commit_receipt_claims_new_state(commit_receipt)
            decision, _, _, projected_new = _project_new_commit_receipt(commit_receipt, preview)
            if projected_new is not state_committed:
                raise ProspectiveDecisionTransactionErrorV2(
                    "strategy commit receipt ownership projection is inconsistent"
                )
            if not state_committed:
                raise ProspectiveDecisionTransactionErrorV2(
                    "strategy entry was committed by a foreign concurrent actor"
                )
            if decision != preview.decision:
                raise ProspectiveDecisionTransactionErrorV2(
                    "strategy commit returned a decision different from its preview"
                )
            disposition = build_prospective_cell_disposition_payload_from_receipts_v2(
                prepare=prepare,
                prepare_durable_receipt=prepare_receipt,
                commit_receipt=commit_receipt,
                decision_receipt=decision_receipt,
            )
            disposition_jsonl = canonical_prospective_cell_disposition_payload_v2(disposition)
            self._run_fault_hook(ProspectiveDecisionTransactionStageV2.AFTER_STATE_COMMIT)
            disposition_append_started = True
            disposition_receipt = self._store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
                canonical_payload_jsonl=disposition_jsonl,
                transaction_claim=self._transaction_claim,
            )
            disposition_durable = True
            _assert_single_durable_record(
                disposition_receipt,
                kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
                cell_id=prepare.cell_id,
                canonical_payload_jsonl=disposition_jsonl,
            )
            result = ProspectiveDecisionTransactionResultV2(
                decision=decision,
                decision_receipt=decision_receipt,
                prepare_payload=prepare,
                disposition_payload=disposition,
                prepare_durable_receipt=prepare_receipt,
                disposition_durable_receipt=disposition_receipt,
                _factory_token=_RESULT_FACTORY_TOKEN,
            )
            self._advance_cell_watermark_guarded(cell)
            self._run_fault_hook(ProspectiveDecisionTransactionStageV2.AFTER_DISPOSITION_DURABLE)
            return result
        except BaseException as error:
            if disposition_durable:
                self._failed = error
                raise
            if not state_committed:
                if commit_returned:
                    self._failed = error
                raise
            if disposition_append_started:
                indeterminate = ProspectiveDecisionTransactionIndeterminateErrorV2(
                    "DISPOSITION append started without an exact durable result; "
                    "strategy state is retained for fail-stop typed replay"
                )
                self._failed = indeterminate
                raise indeterminate from error
            try:
                if commit_receipt is None:
                    raise ProspectiveDecisionTransactionErrorV2(
                        "strategy rollback lacks its exact commit receipt"
                    )
                rolled_back = rollback(commit_receipt)
                if not rolled_back:
                    raise ProspectiveDecisionTransactionErrorV2(
                        "strategy rollback did not remove the committed entry"
                    )
            except BaseException as rollback_error:
                self._failed = rollback_error
                raise BaseExceptionGroup(
                    "decision DISPOSITION failed and strategy rollback failed",
                    [error, rollback_error],
                ) from error
            raise

    def _run_fault_hook(
        self,
        stage: ProspectiveDecisionTransactionStageV2,
    ) -> None:
        hook = self._fault_hook
        if hook is not None:
            hook(stage)


def build_prospective_decision_transaction_owner_v2(
    *,
    census_plan: ProspectiveCensusPlanV2,
    writer_lease: WriterLease,
    store: ProspectiveDailyWalStoreV2,
    receipt_clock: ReceiptClock,
    family_a: FamilyAEpisodeLedgerV2,
    family_b: FamilyBDecisionRegistryV2,
    family_c: FamilyCEpisodeLedgerV2,
    fault_hook: ProspectiveDecisionFaultHookV2 | None = None,
) -> ProspectiveDecisionTransactionOwnerV2:
    """Bind one coordinator to the exact plan, lease, store, and ledgers."""

    canonical_prospective_census_plan_v2(census_plan)
    if type(writer_lease) is not WriterLease:
        raise ProspectiveDecisionTransactionErrorV2("writer_lease must be exact WriterLease")
    if type(store) is not ProspectiveDailyWalStoreV2:
        raise ProspectiveDecisionTransactionErrorV2(
            "store must be exact ProspectiveDailyWalStoreV2"
        )
    if not callable(getattr(receipt_clock, "capture", None)):
        raise ProspectiveDecisionTransactionErrorV2("receipt_clock must provide capture()")
    if type(family_a) is not FamilyAEpisodeLedgerV2:
        raise ProspectiveDecisionTransactionErrorV2("family_a must be exact FamilyAEpisodeLedgerV2")
    if type(family_b) is not FamilyBDecisionRegistryV2:
        raise ProspectiveDecisionTransactionErrorV2(
            "family_b must be exact FamilyBDecisionRegistryV2"
        )
    if type(family_c) is not FamilyCEpisodeLedgerV2:
        raise ProspectiveDecisionTransactionErrorV2("family_c must be exact FamilyCEpisodeLedgerV2")
    required_capacity = (
        census_plan.expected_bar_count
        * len(census_plan.symbols)
        * PROSPECTIVE_FAMILY_LEDGER_EVENT_CAPACITY_MULTIPLIER_V2
    )
    if (
        family_a.maximum_events,
        family_b.maximum_events,
        family_c.maximum_events,
    ) != (required_capacity, required_capacity, required_capacity):
        raise ProspectiveDecisionTransactionErrorV2(
            "family ledger capacities differ from the frozen prospective bound"
        )
    if fault_hook is not None and not callable(fault_hook):
        raise ProspectiveDecisionTransactionErrorV2("fault_hook must be callable or None")
    family_authorities = _claim_fresh_family_authorities_v2(
        family_a=family_a,
        family_b=family_b,
        family_c=family_c,
    )
    try:
        with writer_lease.operation_guard():
            transaction_claim = store._claim_decision_transaction_owner_v2(  # pyright: ignore[reportPrivateUsage]
                census_plan=census_plan,
                writer_lease=writer_lease,
            )
    except BaseException as error:
        _release_unconsumed_family_authorities_v2(
            family_a=family_a,
            family_a_authority=family_authorities[0],
            family_b=family_b,
            family_b_authority=family_authorities[1],
            family_c=family_c,
            family_c_authority=family_authorities[2],
            original_error=error,
        )
        raise
    return ProspectiveDecisionTransactionOwnerV2(
        census_plan=census_plan,
        writer_lease=writer_lease,
        store=store,
        receipt_clock=receipt_clock,
        family_a=family_a,
        family_a_authority=family_authorities[0],
        family_b=family_b,
        family_b_authority=family_authorities[1],
        family_c=family_c,
        family_c_authority=family_authorities[2],
        fault_hook=fault_hook,
        transaction_claim=transaction_claim,
        _factory_token=_OWNER_FACTORY_TOKEN,
    )


def _claim_fresh_family_authorities_v2(
    *,
    family_a: FamilyAEpisodeLedgerV2,
    family_b: FamilyBDecisionRegistryV2,
    family_c: FamilyCEpisodeLedgerV2,
) -> tuple[object, object, object]:
    """Claim all exact-genesis family owners before touching the daily store."""

    claims: list[tuple[Callable[[object], None], object]] = []
    try:
        family_a_authority = family_a._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]
        claims.append(
            (
                family_a._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
                family_a_authority,
            )
        )
        family_b_authority = family_b._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]
        claims.append(
            (
                family_b._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
                family_b_authority,
            )
        )
        family_c_authority = family_c._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]
        claims.append(
            (
                family_c._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
                family_c_authority,
            )
        )
    except BaseException as error:
        release_errors = _release_claims_reverse_v2(claims)
        if release_errors:
            raise BaseExceptionGroup(
                "family authority claim failed and an earlier empty claim could not release",
                [error, *release_errors],
            ) from error
        raise
    return family_a_authority, family_b_authority, family_c_authority


def _release_unconsumed_family_authorities_v2(
    *,
    family_a: FamilyAEpisodeLedgerV2,
    family_a_authority: object,
    family_b: FamilyBDecisionRegistryV2,
    family_b_authority: object,
    family_c: FamilyCEpisodeLedgerV2,
    family_c_authority: object,
    original_error: BaseException,
) -> None:
    claims: list[tuple[Callable[[object], None], object]] = [
        (
            family_a._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
            family_a_authority,
        ),
        (
            family_b._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
            family_b_authority,
        ),
        (
            family_c._release_unconsumed_prospective_decision_authority_v2,  # pyright: ignore[reportPrivateUsage]
            family_c_authority,
        ),
    ]
    release_errors = _release_claims_reverse_v2(claims)
    if release_errors:
        raise BaseExceptionGroup(
            "daily WAL claim failed and empty family authorities could not release",
            [original_error, *release_errors],
        ) from original_error


def _release_claims_reverse_v2(
    claims: list[tuple[Callable[[object], None], object]],
) -> list[BaseException]:
    errors: list[BaseException] = []
    for release, authority in reversed(claims):
        try:
            release(authority)
        except BaseException as error:
            errors.append(error)
    return errors


def _validate_receipt_before_commit(
    receipt: ReceiptTimestamp,
    prepare: ProspectiveDecisionPreparePayloadV2,
) -> None:
    if type(receipt) is not ReceiptTimestamp:
        raise ProspectiveDecisionTransactionErrorV2(
            "receipt_clock returned a non-exact ReceiptTimestamp"
        )
    if (
        type(receipt.received_at_ms) is not int
        or receipt.received_at_ms < prepare.decision_cutoff_ms
        or receipt.received_at_ms > _MAX_CANONICAL_INTEGER_V2
    ):
        raise ProspectiveDecisionTransactionErrorV2(
            "decision receipt wall time must be canonical and at/after cutoff"
        )
    if (
        type(receipt.received_monotonic_ns) is not int
        or receipt.received_monotonic_ns < 0
        or receipt.received_monotonic_ns > _MAX_MONOTONIC_NS_V2
    ):
        raise ProspectiveDecisionTransactionErrorV2(
            "decision receipt monotonic time must be a canonical integer"
        )


def _project_new_commit_receipt(
    receipt: FamilyEntryCommitReceiptV2,
    preview: FamilyEntryPreviewV2,
) -> tuple[FamilyEntryDecisionV2, str, int, bool]:
    if type(receipt) is FamilyAEntryCommitReceiptV2 and type(preview) is (FamilyAEntryPreviewV2):
        pre_root = preview.pre_root_sha256
        is_new = receipt.disposition is FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    elif type(receipt) is FamilyBEntryCommitReceiptV2 and type(preview) is (FamilyBEntryPreviewV2):
        pre_root = preview.pre_replay_root_sha256
        is_new = receipt.disposition is FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    elif type(receipt) is FamilyCEntryCommitReceiptV2 and type(preview) is (FamilyCEntryPreviewV2):
        pre_root = preview.pre_root_sha256
        is_new = receipt.disposition is FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    else:
        raise ProspectiveDecisionTransactionErrorV2(
            "strategy commit receipt differs from its exact family preview"
        )
    if (
        receipt.input_sha256 != preview.input_sha256
        or receipt.event_id != preview.decision.event_id
        or receipt.decision != preview.decision
        or receipt.pre_root_sha256 != pre_root
        or receipt.pre_event_count != preview.pre_event_count
    ):
        raise ProspectiveDecisionTransactionErrorV2(
            "strategy commit receipt differs from the durable PREPARE"
        )
    return (
        receipt.decision,
        receipt.post_root_sha256,
        receipt.post_event_count,
        is_new,
    )


def _commit_receipt_claims_new_state(receipt: FamilyEntryCommitReceiptV2) -> bool:
    if type(receipt) is FamilyAEntryCommitReceiptV2:
        return receipt.disposition is FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    if type(receipt) is FamilyBEntryCommitReceiptV2:
        return receipt.disposition is FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    if type(receipt) is FamilyCEntryCommitReceiptV2:
        return receipt.disposition is FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    raise ProspectiveDecisionTransactionErrorV2(
        "strategy commit returned an unrecognized receipt type"
    )


def _assert_single_durable_record(
    receipt: ProspectiveDailyWalDurableBatchReceiptV2,
    *,
    kind: ProspectiveWalRecordKindV2,
    cell_id: str,
    canonical_payload_jsonl: bytes,
) -> ProspectiveDailyWalDurableRecordV2:
    if type(receipt) is not ProspectiveDailyWalDurableBatchReceiptV2:
        raise ProspectiveDecisionTransactionErrorV2(
            "store returned a non-exact durable batch receipt"
        )
    if len(receipt.records) != 1:
        raise ProspectiveDecisionTransactionErrorV2(
            "one decision append must acknowledge exactly one record"
        )
    record = receipt.records[0]
    expected_payload_sha256 = hashlib.sha256(canonical_payload_jsonl).hexdigest()
    if (
        type(record) is not ProspectiveDailyWalDurableRecordV2
        or record.kind is not kind
        or record.cell_id != cell_id
        or record.sizing_cell is not None
        or record.payload_sha256 != expected_payload_sha256
    ):
        raise ProspectiveDecisionTransactionErrorV2(
            "durable record identity differs from the exact decision append"
        )
    if receipt.attempt_plan_sha256 == "" or receipt.segment_id == "":
        raise ProspectiveDecisionTransactionErrorV2(
            "durable decision receipt has an empty attempt or segment identity"
        )
    return record
