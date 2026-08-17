from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.r4b_v2.strategy.cross_sectional_evidence import (
    CROSS_SECTIONAL_CONTEXT_MIN_MEMBERS_V2,
    CrossSectionalContextEvidenceV2,
    CrossSectionalContextStatusV2,
    CrossSectionalEvidenceContractErrorV2,
    build_target_excluded_cross_section_evidence_v2,
    canonical_cross_sectional_context_evidence_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FamilyCCandlePanelV2,
    FamilyCClosedCandleV2,
)

from .test_family_c import (
    BAR_CLOSE_MS,
    BAR_OPEN_MS,
    DECISION_CUTOFF_MS,
    _causal_panel,
    _symbols,
    _universe,
)


def _panel_with_count(count: int) -> FamilyCCandlePanelV2:
    panel = _causal_panel()
    members = set(_symbols(count))
    return FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=_universe(count),
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=tuple(item for item in panel.candles if item.symbol in members),
    )


@cache
def _zero_scale_panel() -> FamilyCCandlePanelV2:
    first_open_ms = BAR_OPEN_MS - (
        (FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * 300_000
    )
    candles: list[FamilyCClosedCandleV2] = []
    for symbol in _symbols():
        source_hash = hashlib.sha256(symbol.encode()).hexdigest()
        for index in range(FAMILY_C_PANEL_BAR_COUNT_V2):
            bar_open_ms = first_open_ms + index * 300_000
            bar_close_ms = bar_open_ms + 299_999
            candles.append(
                FamilyCClosedCandleV2(
                    symbol=symbol,
                    bar_open_ms=bar_open_ms,
                    bar_close_ms=bar_close_ms,
                    event_time_ms=bar_close_ms,
                    receipt_time_ms=(
                        DECISION_CUTOFF_MS
                        if bar_open_ms == BAR_OPEN_MS
                        else bar_close_ms
                    ),
                    close=Decimal(100),
                    source_evidence_sha256=source_hash,
                )
            )
    return FamilyCCandlePanelV2(
        venue=_causal_panel().venue,
        promoting_plan_sha256=_causal_panel().promoting_plan_sha256,
        source_root_sha256=_causal_panel().source_root_sha256,
        universe=_universe(),
        current_bar_open_ms=BAR_OPEN_MS,
        current_bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        candles=tuple(candles),
    )


def test_target_is_removed_before_market_math_and_20_to_19_is_ready() -> None:
    panel = _causal_panel()
    excluded = build_target_excluded_cross_section_evidence_v2(
        panel,
        target_symbol="S00USDT",
    )
    no_op = build_target_excluded_cross_section_evidence_v2(
        panel,
        target_symbol="BTCUSDT",
    )

    assert CROSS_SECTIONAL_CONTEXT_MIN_MEMBERS_V2 == 19
    assert excluded.status is CrossSectionalContextStatusV2.READY
    assert excluded.target_present
    assert len(excluded.ex_target_members) == 19
    assert "S00USDT" not in excluded.ex_target_members
    assert excluded.m3_ex_target != no_op.m3_ex_target
    assert canonical_cross_sectional_context_evidence_v2(excluded)


def test_target_absent_is_a_deterministic_no_op_without_survivor_dropping() -> None:
    panel = _causal_panel()
    first = build_target_excluded_cross_section_evidence_v2(
        panel,
        target_symbol="BTCUSDT",
    )
    second = build_target_excluded_cross_section_evidence_v2(
        panel,
        target_symbol="BTCUSDT",
    )

    assert not first.target_present
    assert first.ex_target_members == panel.universe.members
    assert first.original_member_count == len(first.ex_target_members)
    assert first == second
    assert first.evidence_sha256 == second.evidence_sha256


def test_target_only_candle_change_cannot_change_ex_target_slice_or_freshness() -> None:
    panel = _causal_panel()
    target_symbol = "S00USDT"
    baseline_rows: list[FamilyCClosedCandleV2] = []
    changed_rows: list[FamilyCClosedCandleV2] = []
    for candle in panel.candles:
        baseline = candle
        if candle.bar_open_ms == BAR_OPEN_MS and candle.symbol != target_symbol:
            baseline = replace(candle, receipt_time_ms=DECISION_CUTOFF_MS - 1)
        baseline_rows.append(baseline)
        changed_rows.append(
            replace(baseline, close=baseline.close + Decimal(1))
            if candle.bar_open_ms == BAR_OPEN_MS and candle.symbol == target_symbol
            else baseline
        )

    baseline_panel = FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=panel.universe,
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=tuple(baseline_rows),
    )
    changed_panel = FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=panel.universe,
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=tuple(changed_rows),
    )
    baseline = build_target_excluded_cross_section_evidence_v2(
        baseline_panel,
        target_symbol=target_symbol,
    )
    changed = build_target_excluded_cross_section_evidence_v2(
        changed_panel,
        target_symbol=target_symbol,
    )

    assert baseline.original_panel_root_sha256 != changed.original_panel_root_sha256
    assert baseline.ex_target_slice_root_sha256 == changed.ex_target_slice_root_sha256
    assert baseline.m3_ex_target == changed.m3_ex_target
    assert baseline.shock_score == changed.shock_score
    assert baseline.breadth_count == changed.breadth_count
    assert baseline.latest_source_receipt_ms == DECISION_CUTOFF_MS - 1
    assert changed.latest_source_receipt_ms == DECISION_CUTOFF_MS - 1


def test_19_to_18_fails_closed_without_partial_numeric_context() -> None:
    evidence = build_target_excluded_cross_section_evidence_v2(
        _panel_with_count(19),
        target_symbol="S00USDT",
    )

    assert evidence.status is CrossSectionalContextStatusV2.FEATURE_NOT_READY_MEMBER_COUNT
    assert len(evidence.ex_target_members) == 18
    assert evidence.m3_ex_target is None
    assert evidence.shock_scale is None
    assert evidence.breadth_count is None


def test_original_19_member_panel_is_not_promoted_when_target_is_absent() -> None:
    evidence = build_target_excluded_cross_section_evidence_v2(
        _panel_with_count(19),
        target_symbol="BTCUSDT",
    )

    assert not evidence.target_present
    assert len(evidence.ex_target_members) == 19
    assert evidence.status is CrossSectionalContextStatusV2.FEATURE_NOT_READY_MEMBER_COUNT
    assert evidence.m3_ex_target is None
    assert evidence.shock_score is None


def test_zero_prior_market_scale_fails_closed_without_neutral_fallback() -> None:
    evidence = build_target_excluded_cross_section_evidence_v2(
        _zero_scale_panel(),
        target_symbol="S00USDT",
    )

    assert evidence.status is CrossSectionalContextStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert not evidence.ready
    assert evidence.m3_ex_target is None
    assert evidence.shock_score is None


def test_decimal_overflow_fails_closed_without_partial_numeric_context() -> None:
    panel = _causal_panel()
    hostile_panel = FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=panel.universe,
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=tuple(
            replace(candle, close=Decimal("1E+10000000"))
            if candle.symbol == "S01USDT" and candle.bar_open_ms == BAR_OPEN_MS
            else candle
            for candle in panel.candles
        ),
    )
    evidence = build_target_excluded_cross_section_evidence_v2(
        hostile_panel,
        target_symbol="S00USDT",
    )

    assert evidence.status is CrossSectionalContextStatusV2.DATA_INVALID_ARITHMETIC
    assert evidence.m3_ex_target is None
    assert evidence.shock_scale is None
    assert evidence.breadth_count is None


def test_panel_permutation_is_canonical_and_cutoff_equalities_are_retained() -> None:
    panel = _causal_panel()
    permuted = FamilyCCandlePanelV2(
        venue=panel.venue,
        promoting_plan_sha256=panel.promoting_plan_sha256,
        source_root_sha256=panel.source_root_sha256,
        universe=panel.universe,
        current_bar_open_ms=panel.current_bar_open_ms,
        current_bar_close_ms=panel.current_bar_close_ms,
        decision_cutoff_ms=panel.decision_cutoff_ms,
        candles=tuple(reversed(panel.candles)),
    )
    first = build_target_excluded_cross_section_evidence_v2(
        panel,
        target_symbol="S00USDT",
    )
    second = build_target_excluded_cross_section_evidence_v2(
        permuted,
        target_symbol="S00USDT",
    )

    assert first == second
    assert first.latest_source_event_ms == BAR_CLOSE_MS
    assert first.latest_source_receipt_ms == DECISION_CUTOFF_MS


def test_direct_construction_and_dataclass_replace_cannot_bypass_factory() -> None:
    evidence = build_target_excluded_cross_section_evidence_v2(
        _causal_panel(),
        target_symbol="S00USDT",
    )
    constructor_values = {
        item.name: getattr(evidence, item.name)
        for item in fields(evidence)
        if item.init
    }

    with pytest.raises(CrossSectionalEvidenceContractErrorV2, match="causal factory"):
        CrossSectionalContextEvidenceV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(CrossSectionalEvidenceContractErrorV2, match="causal factory"):
        replace(evidence, target_symbol="S01USDT")
