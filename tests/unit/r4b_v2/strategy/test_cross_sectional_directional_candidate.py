from __future__ import annotations

import json
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.r4b_v2.strategy import cross_sectional_evidence as context_module
from signalbot.r4b_v2.strategy.cross_sectional_directional_candidate import (
    CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_RULE_VERSION_V2,
    CrossSectionalDirectionalCandidateContractErrorV2,
    CrossSectionalDirectionalCandidateStatusV2,
    CrossSectionalDirectionalCandidateV2,
    build_cross_sectional_directional_candidate_v2,
    canonical_cross_sectional_directional_candidate_v2,
)
from signalbot.r4b_v2.strategy.cross_sectional_evidence import (
    CrossSectionalContextEvidenceV2,
    CrossSectionalContextStatusV2,
    build_target_excluded_cross_section_evidence_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCCandlePanelV2,
    FamilyCClosedCandleV2,
)

from .test_family_c import DECISION_CUTOFF_MS, _causal_panel, _symbols, _universe

SOURCE_FACTORY_TOKEN = vars(context_module)["_FACTORY_TOKEN"]


@cache
def _base_context() -> CrossSectionalContextEvidenceV2:
    context = build_target_excluded_cross_section_evidence_v2(
        _causal_panel(),
        target_symbol="S00USDT",
    )
    assert context.status is CrossSectionalContextStatusV2.READY
    return context


def _ready_context(
    *,
    shock_score: Decimal,
    sign: int,
    breadth_count: int,
) -> CrossSectionalContextEvidenceV2:
    base = _base_context()
    assert sign in (-1, 0, 1)
    return CrossSectionalContextEvidenceV2(
        target_symbol=base.target_symbol,
        target_present=base.target_present,
        venue=base.venue,
        promoting_plan_sha256=base.promoting_plan_sha256,
        source_root_sha256=base.source_root_sha256,
        original_universe_root_sha256=base.original_universe_root_sha256,
        original_panel_root_sha256=base.original_panel_root_sha256,
        bar_open_ms=base.bar_open_ms,
        bar_close_ms=base.bar_close_ms,
        decision_cutoff_ms=base.decision_cutoff_ms,
        latest_source_event_ms=base.latest_source_event_ms,
        latest_source_receipt_ms=base.latest_source_receipt_ms,
        original_member_count=base.original_member_count,
        ex_target_members=base.ex_target_members,
        ex_target_member_root_sha256=base.ex_target_member_root_sha256,
        ex_target_slice_root_sha256=base.ex_target_slice_root_sha256,
        prior_observation_count=base.prior_observation_count,
        status=CrossSectionalContextStatusV2.READY,
        reasons=base.reasons,
        m3_ex_target=Decimal(sign) * shock_score,
        shock_scale=Decimal(1),
        shock_score=shock_score,
        breadth_count=breadth_count,
        breadth_denominator=len(base.ex_target_members),
        _factory_token=SOURCE_FACTORY_TOKEN,
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


def test_sign_symmetry_and_exact_decimal34_floor() -> None:
    positive = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=1,
            breadth_count=1,
        )
    )
    negative = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=-1,
            breadth_count=1,
        )
    )

    assert positive.shock_magnitude == negative.shock_magnitude == Decimal("0.5")
    assert positive.breadth_support == Decimal(
        "0.05263157894736842105263157894736842"
    )
    assert positive.strength_micros == negative.strength_micros == 26_315
    assert positive.direction == 1
    assert negative.direction == -1
    assert positive.signed_strength_micros == 26_315
    assert negative.signed_strength_micros == -26_315


def test_scale_zero_and_breadth_boundaries_are_frozen() -> None:
    full_breadth = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=1,
            breadth_count=19,
        )
    )
    zero_shock = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(0),
            sign=0,
            breadth_count=0,
        )
    )
    zero_breadth = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=-1,
            breadth_count=0,
        )
    )
    sub_quantum = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal("1E-36"),
            sign=1,
            breadth_count=19,
        )
    )
    decimal34_saturation = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal("1E+34"),
            sign=1,
            breadth_count=19,
        )
    )

    assert full_breadth.breadth_support == Decimal(1)
    assert full_breadth.strength_micros == 500_000
    assert zero_shock.shock_magnitude == Decimal(0)
    assert zero_shock.direction == zero_shock.strength_micros == 0
    assert zero_breadth.breadth_support == Decimal(0)
    assert zero_breadth.direction == -1
    assert zero_breadth.strength_micros == 0
    assert sub_quantum.shock_magnitude == Decimal("1E-36")
    assert sub_quantum.direction == 1
    assert sub_quantum.strength_micros == 0
    assert decimal34_saturation.shock_magnitude == Decimal(1)
    assert decimal34_saturation.strength_micros == 1_000_000


def test_nonready_source_is_withheld_not_a_neutral_numeric_fallback() -> None:
    source = build_target_excluded_cross_section_evidence_v2(
        _panel_with_count(19),
        target_symbol="S00USDT",
    )
    candidate = build_cross_sectional_directional_candidate_v2(source)

    assert source.status is CrossSectionalContextStatusV2.FEATURE_NOT_READY_MEMBER_COUNT
    assert candidate.status is CrossSectionalDirectionalCandidateStatusV2.NOT_READY
    assert not candidate.ready
    assert candidate.direction == candidate.strength_micros == 0
    assert candidate.signed_strength_micros == 0
    assert candidate.m3_ex_target is None
    assert candidate.shock_magnitude is None
    assert candidate.breadth_support is None
    assert "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK" in candidate.reasons


def test_target_exclusion_roots_are_bound_and_target_only_change_is_invariant() -> None:
    panel = _causal_panel()
    target_symbol = "S00USDT"
    baseline_rows: list[FamilyCClosedCandleV2] = []
    changed_rows: list[FamilyCClosedCandleV2] = []
    for candle in panel.candles:
        baseline = candle
        if candle.bar_open_ms == panel.current_bar_open_ms and candle.symbol != target_symbol:
            baseline = replace(candle, receipt_time_ms=DECISION_CUTOFF_MS - 1)
        baseline_rows.append(baseline)
        changed_rows.append(
            replace(baseline, close=baseline.close + Decimal(1))
            if candle.bar_open_ms == panel.current_bar_open_ms
            and candle.symbol == target_symbol
            else baseline
        )

    baseline_source = build_target_excluded_cross_section_evidence_v2(
        replace(panel, candles=tuple(baseline_rows)),
        target_symbol=target_symbol,
    )
    changed_source = build_target_excluded_cross_section_evidence_v2(
        replace(panel, candles=tuple(changed_rows)),
        target_symbol=target_symbol,
    )
    baseline = build_cross_sectional_directional_candidate_v2(baseline_source)
    changed = build_cross_sectional_directional_candidate_v2(changed_source)

    assert target_symbol not in baseline_source.ex_target_members
    assert baseline.ex_target_member_root_sha256 == (
        baseline_source.ex_target_member_root_sha256
    )
    assert baseline.ex_target_slice_root_sha256 == (
        baseline_source.ex_target_slice_root_sha256
    )
    assert baseline_source.ex_target_slice_root_sha256 == (
        changed_source.ex_target_slice_root_sha256
    )
    assert baseline_source.m3_ex_target == changed_source.m3_ex_target
    assert baseline_source.shock_score == changed_source.shock_score
    assert baseline_source.breadth_count == changed_source.breadth_count
    assert baseline.direction == changed.direction
    assert baseline.strength_micros == changed.strength_micros
    assert baseline.source_context_sha256 != changed.source_context_sha256
    assert baseline.candidate_sha256 != changed.candidate_sha256


def test_source_tamper_and_candidate_tamper_fail_closed() -> None:
    source = _ready_context(
        shock_score=Decimal(1),
        sign=1,
        breadth_count=19,
    )
    object.__setattr__(source, "shock_score", Decimal(2))
    with pytest.raises(
        CrossSectionalDirectionalCandidateContractErrorV2,
        match="source_context failed canonical validation",
    ):
        build_cross_sectional_directional_candidate_v2(source)

    candidate = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=1,
            breadth_count=19,
        )
    )
    object.__setattr__(candidate, "direction", -1)
    with pytest.raises(
        CrossSectionalDirectionalCandidateContractErrorV2,
        match="contradict the bound source context",
    ):
        canonical_cross_sectional_directional_candidate_v2(candidate)


def test_direct_construction_and_replace_cannot_bypass_candidate_factory() -> None:
    candidate = build_cross_sectional_directional_candidate_v2(
        _ready_context(
            shock_score=Decimal(1),
            sign=1,
            breadth_count=19,
        )
    )
    constructor_values = {
        item.name: getattr(candidate, item.name)
        for item in fields(candidate)
        if item.init
    }

    with pytest.raises(
        CrossSectionalDirectionalCandidateContractErrorV2,
        match="frozen factory",
    ):
        CrossSectionalDirectionalCandidateV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(
        CrossSectionalDirectionalCandidateContractErrorV2,
        match="frozen factory",
    ):
        replace(candidate, strength_micros=1)


def test_canonical_hash_is_deterministic_and_binds_source_reasons_and_clocks() -> None:
    source = _ready_context(
        shock_score=Decimal(1),
        sign=1,
        breadth_count=19,
    )
    first = build_cross_sectional_directional_candidate_v2(source)
    second = build_cross_sectional_directional_candidate_v2(source)
    first_payload = canonical_cross_sectional_directional_candidate_v2(first)
    second_payload = canonical_cross_sectional_directional_candidate_v2(second)
    document = json.loads(first_payload)

    assert first.event_id == second.event_id
    assert first.candidate_sha256 == second.candidate_sha256
    assert first_payload == second_payload
    assert document["source_context_sha256"] == source.evidence_sha256
    assert document["source_context_reasons"] == list(source.reasons)
    assert document["ex_target_member_root_sha256"] == (
        source.ex_target_member_root_sha256
    )
    assert document["ex_target_slice_root_sha256"] == (
        source.ex_target_slice_root_sha256
    )
    assert document["bar_open_ms"] == source.bar_open_ms
    assert document["bar_close_ms"] == source.bar_close_ms
    assert document["decision_cutoff_ms"] == source.decision_cutoff_ms
    assert document["rule_version"] == (
        CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_RULE_VERSION_V2
    )


def test_candidate_cannot_claim_probability_promotion_authority_or_outcomes() -> None:
    candidate = build_cross_sectional_directional_candidate_v2(_base_context())
    document = json.loads(canonical_cross_sectional_directional_candidate_v2(candidate))

    assert document["shadow_only"] is True
    assert document["pre_outcome_frozen"] is True
    assert document["verified_raw_membership_m0_bound"] is False
    assert document["strict_source_parser_m1_bound"] is False
    assert document["causal_cursor_finality_m2_bound"] is False
    assert document["causal_inputs_complete"] is False
    assert document["producer_ready"] is False
    assert document["promoting"] is False
    assert document["probability"] is False
    assert document["probability_calibrated"] is False
    assert document["target_return_used"] is False
    assert document["primary_direction_used"] is False
    assert document["outcome_used"] is False
    assert document["data_through_ms"] is None
    assert "target_return" not in document
    assert "primary_direction" not in document
    assert "outcome" not in document
