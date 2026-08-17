from __future__ import annotations

import pytest

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalPrefixProofV2
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveFamilyRuleBindingV2,
)
from signalbot.r4b_v2.execution.prospective_segment_seal import (
    ProspectiveSegmentSealStatusV2,
    ProspectiveTerminalKeyV2,
    build_prospective_segment_seal_v2,
    canonical_prospective_segment_seal_v2,
)
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 50_000 * DAY_MS


def _plan() -> ProspectiveCensusPlanV2:
    return ProspectiveCensusPlanV2(
        attempt_id="attempt-001",
        attempt=ProspectiveAttemptV2(
            attempt_index=1,
            qualification_start_ms=H_START_MS - 30 * DAY_MS,
            horizon=FixedHorizonV2(h_start_ms=H_START_MS),
        ),
        promoting_plan_sha256="a" * 64,
        symbols=("BTCUSDT",),
        context_symbols=(*tuple(f"C{index:02d}USDT" for index in range(20)), "BTCUSDT"),
        family_rules=(
            ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.A, "a-v2"),
            ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.B, "b-v2"),
            ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.C, "c-v2"),
        ),
        paper_fok_rule_version="paper-v2",
        execution_contract_sha256="b" * 64,
        efficacy_gate_contract_sha256="c" * 64,
        strategy_code_freeze_manifest_sha256="d" * 64,
        created_at_ms=H_START_MS - 1,
    )


def _proof(record_count: int) -> MirroredWalPrefixProofV2:
    return MirroredWalPrefixProofV2(
        durable_ack_seq=record_count,
        record_count=record_count,
        prefix_sha256="e" * 64,
        durability_binding_sha256="f" * 64,
        selection_receipt_sha256="1" * 64,
    )


def test_complete_segment_requires_every_cell_and_both_sizing_terminals() -> None:
    plan = _plan()
    segment = next(plan.iter_segments())
    cells = tuple(plan.iter_expected_cells_for_segment(segment))
    cell_ids = tuple(cell.cell_id for cell in cells)
    signal_id = cell_ids[0]
    terminals = tuple(
        ProspectiveTerminalKeyV2(signal_id, sizing_cell)
        for sizing_cell in PaperSizingCellV2
    )
    record_count = len(cell_ids) * 2 + len(terminals)

    seal = build_prospective_segment_seal_v2(
        plan=plan,
        segment=segment,
        segment_index=0,
        previous_segment_seal_sha256=None,
        wal_authority_sha256="2" * 64,
        wal_prefix_proof=_proof(record_count),
        wal_tail_record_sha256="3" * 64,
        prepared_cell_ids=cell_ids,
        disposition_cell_ids=cell_ids,
        signal_cell_ids=(signal_id,),
        paper_terminal_keys=terminals,
    )

    assert seal.status is ProspectiveSegmentSealStatusV2.COMPLETE
    assert seal.expected_cell_count == segment.expected_cell_count == 864
    assert seal.missing_disposition_count == 0
    assert seal.pending_terminal_count == 0
    assert not seal.durable_storage_authority_established
    assert not seal.efficacy_pass_authorized
    assert not seal.production_order_authorized
    assert canonical_prospective_segment_seal_v2(seal).endswith(b"\n")


def test_empty_missing_day_is_incomplete_and_remains_in_denominator() -> None:
    plan = _plan()
    segment = next(plan.iter_segments())
    seal = build_prospective_segment_seal_v2(
        plan=plan,
        segment=segment,
        segment_index=0,
        previous_segment_seal_sha256=None,
        wal_authority_sha256="2" * 64,
        wal_prefix_proof=_proof(0),
        wal_tail_record_sha256=None,
        prepared_cell_ids=(),
        disposition_cell_ids=(),
        signal_cell_ids=(),
        paper_terminal_keys=(),
    )

    assert seal.status is ProspectiveSegmentSealStatusV2.INCOMPLETE
    assert seal.expected_cell_count == segment.expected_cell_count
    assert seal.missing_disposition_count == segment.expected_cell_count
    assert seal.wal_record_count == 0


def test_one_missing_sizing_terminal_prevents_complete_status() -> None:
    plan = _plan()
    segment = next(plan.iter_segments())
    cells = tuple(plan.iter_expected_cells_for_segment(segment))
    cell_ids = tuple(cell.cell_id for cell in cells)
    signal_id = cell_ids[0]
    only_one = (
        ProspectiveTerminalKeyV2(
            signal_id,
            PaperSizingCellV2.NOTIONAL_100_USDT,
        ),
    )
    seal = build_prospective_segment_seal_v2(
        plan=plan,
        segment=segment,
        segment_index=0,
        previous_segment_seal_sha256=None,
        wal_authority_sha256="2" * 64,
        wal_prefix_proof=_proof(len(cell_ids) * 2 + 1),
        wal_tail_record_sha256="3" * 64,
        prepared_cell_ids=cell_ids,
        disposition_cell_ids=cell_ids,
        signal_cell_ids=(signal_id,),
        paper_terminal_keys=only_one,
    )

    assert seal.status is ProspectiveSegmentSealStatusV2.INCOMPLETE
    assert seal.pending_terminal_count == 1


def test_unaccounted_durable_wal_record_fails_closed() -> None:
    plan = _plan()
    segment = next(plan.iter_segments())
    cell = next(plan.iter_expected_cells_for_segment(segment))

    with pytest.raises(ValueError, match="WAL record count differs"):
        build_prospective_segment_seal_v2(
            plan=plan,
            segment=segment,
            segment_index=0,
            previous_segment_seal_sha256=None,
            wal_authority_sha256="2" * 64,
            wal_prefix_proof=_proof(3),
            wal_tail_record_sha256="3" * 64,
            prepared_cell_ids=(cell.cell_id,),
            disposition_cell_ids=(cell.cell_id,),
            signal_cell_ids=(),
            paper_terminal_keys=(),
        )
