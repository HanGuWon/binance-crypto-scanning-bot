from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

import signalbot.r4b_v2.execution.prospective_paper_terminal_payload as terminal_payload_module
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    ProspectiveDailyWalAppendItemV2,
    ProspectiveDailyWalStoreContractErrorV2,
    ProspectiveDailyWalStoreFactoryV2,
    ProspectiveDailyWalStoreIntegrityErrorV2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    ProspectivePaperTerminalPayloadV2,
    ProspectivePaperTerminalStatusV2,
    build_prospective_paper_terminal_payload_v2,
    canonical_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    ProspectiveWalRecordKindV2,
)
from signalbot.r4b_v2.strategy.family_a_features import FamilyAFeatureReadinessV2

from ..strategy import test_family_a as family_a_testkit
from . import test_prospective_daily_wal_store as store_testkit
from . import test_prospective_paper_terminal_payload as terminal_testkit


def _no_signal_item():
    return family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
        family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
            rz_r12_previous=Decimal("1.49")
        )
    )


def _open_no_signal_transaction(tmp_path: Path):
    return terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        _no_signal_item(),
    )


def _terminal(plan, cell, transaction, sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT):
    return build_prospective_paper_terminal_payload_v2(
        plan=plan,
        cell=cell,
        transaction=transaction,
        sizing_cell=sizing_cell,
    )


def _rehash_mutated(payload: ProspectivePaperTerminalPayloadV2) -> None:
    object.__setattr__(
        payload,
        "payload_sha256",
        terminal_payload_module._hash_document(  # pyright: ignore[reportPrivateUsage]
            terminal_payload_module._payload_document(  # pyright: ignore[reportPrivateUsage]
                payload
            )
        ),
    )


def _abort_and_release(store, lease: WriterLease) -> None:
    store.abort()
    lease.release()


def test_typed_terminal_requires_sealed_payload_and_invalid_admission_is_atomic(
    tmp_path: Path,
) -> None:
    plan, cell, transaction, store, lease = _open_no_signal_transaction(tmp_path)
    payload = _terminal(plan, cell, transaction)
    encoded = canonical_prospective_paper_terminal_payload_v2(payload)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="exact factory-sealed payload",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=encoded,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            )

        # A decision-owner claim never authorizes a terminal or a mixed batch.
        claim = store._decision_transaction_claim  # pyright: ignore[reportPrivateUsage]
        assert claim is not None
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="one exact decision record only",
        ):
            store.append_batch_and_sync(
                (
                    ProspectiveDailyWalAppendItemV2(
                        cell=cell,
                        kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                        canonical_payload_jsonl=encoded,
                        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                        typed_paper_terminal=payload,
                    ),
                    ProspectiveDailyWalAppendItemV2(
                        cell=plan.expected_cell(
                            family=PromotingFamilyV2.A,
                            symbol=cell.symbol,
                            bar_open_ms=cell.bar_open_ms + 300_000,
                        ),
                        kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                        canonical_payload_jsonl=b"{}\n",
                    ),
                ),
                transaction_claim=claim,
            )

        receipt = store.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            canonical_payload_jsonl=encoded,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            typed_paper_terminal=payload,
        )
        # The failed admissions did not consume sequence 3 or mutate cell state.
        assert receipt.first_ingest_seq == receipt.last_ingest_seq == 3
        assert receipt.records[0].payload_sha256

        capacity_payload = _terminal(
            plan,
            cell,
            transaction,
            PaperSizingCellV2.NOTIONAL_1000_USDT,
        )
        capacity_receipt = store.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            canonical_payload_jsonl=canonical_prospective_paper_terminal_payload_v2(
                capacity_payload
            ),
            sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
            typed_paper_terminal=capacity_payload,
        )
        assert capacity_receipt.first_ingest_seq == capacity_receipt.last_ingest_seq == 4

        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="duplicate PAPER_TERMINAL",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=encoded,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                typed_paper_terminal=payload,
            )
    finally:
        _abort_and_release(store, lease)


def test_typed_terminal_binds_durable_transition_cell_and_status(
    tmp_path: Path,
) -> None:
    plan, cell, transaction, store, lease = _open_no_signal_transaction(tmp_path / "local")
    try:
        foreign_item = family_a_testkit._entry_input(  # pyright: ignore[reportPrivateUsage]
            family_a_testkit._trusted_entry_evidence(  # pyright: ignore[reportPrivateUsage]
                readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
                reasons=("TEST_SOURCE_INCOMPLETE",),
            )
        )
        (
            foreign_plan,
            foreign_cell,
            foreign_transaction,
            foreign_store,
            foreign_lease,
        ) = terminal_testkit._transact(  # pyright: ignore[reportPrivateUsage]
            tmp_path / "foreign",
            foreign_item,
        )
        try:
            foreign_payload = _terminal(
                foreign_plan,
                foreign_cell,
                foreign_transaction,
            )
            with pytest.raises(
                ProspectiveDailyWalStoreContractErrorV2,
                match="durable PREPARE/DISPOSITION identities",
            ):
                store.append_and_sync(
                    cell=cell,
                    kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                    canonical_payload_jsonl=(
                        canonical_prospective_paper_terminal_payload_v2(foreign_payload)
                    ),
                    sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                    typed_paper_terminal=foreign_payload,
                )
        finally:
            _abort_and_release(foreign_store, foreign_lease)

        next_cell = plan.expected_cell(
            family=PromotingFamilyV2.A,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms + 300_000,
        )
        local_payload = _terminal(plan, cell, transaction)
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="previously durable disposition",
        ):
            store.append_and_sync(
                cell=next_cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=(
                    canonical_prospective_paper_terminal_payload_v2(local_payload)
                ),
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                typed_paper_terminal=local_payload,
            )

        status_tamper = _terminal(plan, cell, transaction)
        object.__setattr__(
            status_tamper,
            "terminal_status",
            ProspectivePaperTerminalStatusV2.SUPPRESSED_DECISION,
        )
        _rehash_mutated(status_tamper)
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="status is incompatible",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=(
                    canonical_prospective_paper_terminal_payload_v2(status_tamper)
                ),
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                typed_paper_terminal=status_tamper,
            )

        record_tamper = _terminal(plan, cell, transaction)
        object.__setattr__(record_tamper, "prepare_record_sha256", "f" * 64)
        _rehash_mutated(record_tamper)
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="durable PREPARE/DISPOSITION identities",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=(
                    canonical_prospective_paper_terminal_payload_v2(record_tamper)
                ),
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                typed_paper_terminal=record_tamper,
            )

        valid = _terminal(plan, cell, transaction)
        receipt = store.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            canonical_payload_jsonl=canonical_prospective_paper_terminal_payload_v2(valid),
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            typed_paper_terminal=valid,
        )
        assert receipt.first_ingest_seq == 3
    finally:
        _abort_and_release(store, lease)


def test_typed_terminal_replay_fails_closed_without_live_factory_sources(
    tmp_path: Path,
) -> None:
    plan, cell, transaction, first, first_lease = _open_no_signal_transaction(tmp_path)
    payload = _terminal(plan, cell, transaction)
    first.append_and_sync(
        cell=cell,
        kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
        canonical_payload_jsonl=canonical_prospective_paper_terminal_payload_v2(payload),
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        typed_paper_terminal=payload,
    )
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
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="cannot reconstruct its live factory-sealed transaction",
        ):
            second.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                canonical_payload_jsonl=canonical_prospective_paper_terminal_payload_v2(payload),
                sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
                typed_paper_terminal=_terminal(
                    plan,
                    cell,
                    transaction,
                    PaperSizingCellV2.NOTIONAL_1000_USDT,
                ),
            )
    finally:
        second.abort()
        second_lease.release()
