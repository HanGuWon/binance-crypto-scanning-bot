from __future__ import annotations

import pytest

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusContractErrorV2,
    ProspectiveCensusPlanV2,
    ProspectiveFamilyRuleBindingV2,
    canonical_prospective_census_plan_v2,
    canonical_prospective_census_segment_v2,
    canonical_prospective_expected_cell_v2,
)
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    AttemptTerminalStatusV2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 20_000 * DAY_MS
QUALIFICATION_START_MS = H_START_MS - 30 * DAY_MS
PROMOTING_PLAN_SHA256 = "a" * 64
EXECUTION_CONTRACT_SHA256 = "b" * 64
STRATEGY_CODE_FREEZE_SHA256 = "c" * 64
EFFICACY_GATE_CONTRACT_SHA256 = "e" * 64
CONTEXT_FILLERS = tuple(f"C{index:02d}USDT" for index in range(20))
RULES = (
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.A, "family-a-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.B, "family-b-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.C, "family-c-v2"),
)


def _plan(
    *,
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
    context_symbols: tuple[str, ...] | None = None,
    rules: tuple[ProspectiveFamilyRuleBindingV2, ...] = RULES,
    created_at_ms: int = H_START_MS - 1,
) -> ProspectiveCensusPlanV2:
    context = (
        tuple(sorted(set(symbols).union(CONTEXT_FILLERS)))
        if context_symbols is None
        else context_symbols
    )
    return ProspectiveCensusPlanV2(
        attempt_id="prospective-attempt-001",
        attempt=ProspectiveAttemptV2(
            attempt_index=1,
            qualification_start_ms=QUALIFICATION_START_MS,
            horizon=FixedHorizonV2(h_start_ms=H_START_MS),
        ),
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        symbols=symbols,
        context_symbols=context,
        family_rules=rules,
        paper_fok_rule_version="paper-fok-v2",
        execution_contract_sha256=EXECUTION_CONTRACT_SHA256,
        efficacy_gate_contract_sha256=EFFICACY_GATE_CONTRACT_SHA256,
        strategy_code_freeze_manifest_sha256=STRATEGY_CODE_FREEZE_SHA256,
        created_at_ms=created_at_ms,
    )


def test_365_day_ten_symbol_grid_is_complete_without_one_file_capacity_ceiling() -> None:
    symbols = tuple(f"A{index:02d}USDT" for index in range(10))
    plan = _plan(symbols=symbols)
    segments = tuple(plan.iter_segments())

    assert plan.segment_count == 365
    assert plan.expected_bar_count == 105_104
    assert plan.expected_cell_count == 3_153_120
    assert len(segments) == 365
    assert sum(segment.expected_bar_count for segment in segments) == 105_104
    assert sum(segment.expected_cell_count for segment in segments) == 3_153_120
    assert max(segment.expected_cell_count for segment in segments) == 8_640
    assert segments[0].expected_bar_count == 288
    assert segments[-1].expected_bar_count == 272


def test_first_and_final_admitted_cells_obey_exact_closed_bar_clock() -> None:
    plan = _plan()
    first = plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol="BTCUSDT",
        bar_open_ms=H_START_MS,
    )
    final_segment = tuple(plan.iter_segments())[-1]
    final_open_ms = (
        final_segment.bar_open_stop_exclusive_ms - FIVE_MINUTE_MS_V2
    )
    final = plan.expected_cell(
        family=PromotingFamilyV2.C,
        symbol="ETHUSDT",
        bar_open_ms=final_open_ms,
    )

    assert first.bar_close_ms == H_START_MS + FIVE_MINUTE_MS_V2 - 1
    assert first.decision_cutoff_ms == H_START_MS + FIVE_MINUTE_MS_V2 + 2_000
    assert first.segment_id == next(plan.iter_segments()).segment_id
    assert plan.attempt.horizon.admits_decision(final.decision_cutoff_ms)
    with pytest.raises(
        ProspectiveCensusContractErrorV2,
        match="outside the frozen admission grid",
    ):
        plan.expected_cell(
            family=PromotingFamilyV2.C,
            symbol="ETHUSDT",
            bar_open_ms=final_open_ms + FIVE_MINUTE_MS_V2,
        )


def test_daily_cell_iteration_is_bounded_unique_and_canonical() -> None:
    plan = _plan()
    first_segment = next(plan.iter_segments())
    cells = tuple(plan.iter_expected_cells_for_segment(first_segment))

    assert len(cells) == first_segment.expected_cell_count == 1_728
    assert len({cell.cell_id for cell in cells}) == len(cells)
    assert canonical_prospective_census_plan_v2(plan).endswith(b"\n")
    assert canonical_prospective_census_segment_v2(first_segment).endswith(b"\n")
    assert canonical_prospective_expected_cell_v2(cells[0]).endswith(b"\n")
    assert (
        cells[0].bar_open_ms,
        cells[0].symbol,
        cells[0].family,
    ) == (H_START_MS, "BTCUSDT", PromotingFamilyV2.A)
    assert (
        cells[-1].bar_open_ms,
        cells[-1].symbol,
        cells[-1].family,
    ) == (
        H_START_MS + 287 * FIVE_MINUTE_MS_V2,
        "ETHUSDT",
        PromotingFamilyV2.C,
    )


def test_symbol_order_is_set_canonical_but_duplicates_and_foreign_cells_fail() -> None:
    forward = _plan(symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    reversed_order = _plan(symbols=("SOLUSDT", "ETHUSDT", "BTCUSDT"))

    assert forward.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert reversed_order.symbols == forward.symbols
    assert reversed_order.universe_sha256 == forward.universe_sha256
    assert reversed_order.plan_sha256 == forward.plan_sha256
    with pytest.raises(ProspectiveCensusContractErrorV2, match="unique"):
        _plan(symbols=("BTCUSDT", "BTCUSDT"))
    with pytest.raises(ProspectiveCensusContractErrorV2, match="normalized"):
        _plan(symbols=("btcusdt",))
    with pytest.raises(ProspectiveCensusContractErrorV2, match="absent"):
        forward.expected_cell(
            family=PromotingFamilyV2.A,
            symbol="XRPUSDT",
            bar_open_ms=H_START_MS,
        )


def test_all_three_family_bindings_are_mandatory_and_version_sensitive() -> None:
    with pytest.raises(ProspectiveCensusContractErrorV2, match="A, B, C"):
        _plan(rules=RULES[:2])
    with pytest.raises(ProspectiveCensusContractErrorV2, match="canonical order"):
        _plan(rules=(RULES[1], RULES[0], RULES[2]))

    changed = _plan(
        rules=(
            RULES[0],
            ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.B, "family-b-v3"),
            RULES[2],
        )
    )
    assert changed.plan_sha256 != _plan().plan_sha256


def test_strategy_code_freeze_manifest_is_mandatory_and_hash_sensitive() -> None:
    baseline = _plan()
    changed = ProspectiveCensusPlanV2(
        attempt_id=baseline.attempt_id,
        attempt=baseline.attempt,
        promoting_plan_sha256=baseline.promoting_plan_sha256,
        symbols=baseline.symbols,
        context_symbols=baseline.context_symbols,
        family_rules=baseline.family_rules,
        paper_fok_rule_version=baseline.paper_fok_rule_version,
        execution_contract_sha256=baseline.execution_contract_sha256,
        efficacy_gate_contract_sha256=baseline.efficacy_gate_contract_sha256,
        strategy_code_freeze_manifest_sha256="d" * 64,
        created_at_ms=baseline.created_at_ms,
    )

    assert changed.plan_sha256 != baseline.plan_sha256
    with pytest.raises(
        ProspectiveCensusContractErrorV2,
        match="strategy_code_freeze_manifest_sha256",
    ):
        ProspectiveCensusPlanV2(
            attempt_id=baseline.attempt_id,
            attempt=baseline.attempt,
            promoting_plan_sha256=baseline.promoting_plan_sha256,
            symbols=baseline.symbols,
            context_symbols=baseline.context_symbols,
            family_rules=baseline.family_rules,
            paper_fok_rule_version=baseline.paper_fok_rule_version,
            execution_contract_sha256=baseline.execution_contract_sha256,
            efficacy_gate_contract_sha256=baseline.efficacy_gate_contract_sha256,
            strategy_code_freeze_manifest_sha256="not-a-sha",
            created_at_ms=baseline.created_at_ms,
        )


def test_evaluation_symbols_must_belong_to_context_universe() -> None:
    with pytest.raises(ProspectiveCensusContractErrorV2, match="subset"):
        _plan(
            symbols=("BTCUSDT", "ETHUSDT"),
            context_symbols=("BTCUSDT",),
        )


def test_terminal_attempt_cannot_receive_a_new_census_plan() -> None:
    baseline = _plan()
    with pytest.raises(ProspectiveCensusContractErrorV2, match="terminal"):
        ProspectiveCensusPlanV2(
            attempt_id=baseline.attempt_id,
            attempt=ProspectiveAttemptV2(
                attempt_index=baseline.attempt.attempt_index,
                qualification_start_ms=baseline.attempt.qualification_start_ms,
                horizon=baseline.attempt.horizon,
                terminal_status=AttemptTerminalStatusV2.FAIL,
            ),
            promoting_plan_sha256=baseline.promoting_plan_sha256,
            symbols=baseline.symbols,
            context_symbols=baseline.context_symbols,
            family_rules=baseline.family_rules,
            paper_fok_rule_version=baseline.paper_fok_rule_version,
            execution_contract_sha256=baseline.execution_contract_sha256,
            efficacy_gate_contract_sha256=baseline.efficacy_gate_contract_sha256,
            strategy_code_freeze_manifest_sha256=(
                baseline.strategy_code_freeze_manifest_sha256
            ),
            created_at_ms=baseline.created_at_ms,
        )


@pytest.mark.parametrize(
    "created_at_ms",
    [QUALIFICATION_START_MS - 1, H_START_MS],
)
def test_plan_creation_must_be_pre_h_start_qualification_time(
    created_at_ms: int,
) -> None:
    with pytest.raises(ProspectiveCensusContractErrorV2, match="qualification"):
        _plan(created_at_ms=created_at_ms)


def test_foreign_or_tampered_segment_cannot_drive_cell_enumeration() -> None:
    plan = _plan()
    first = next(plan.iter_segments())
    foreign = _plan(symbols=("BTCUSDT",)).segment_for_day(first.day_start_ms)

    with pytest.raises(ProspectiveCensusContractErrorV2, match="differs"):
        tuple(plan.iter_expected_cells_for_segment(foreign))
