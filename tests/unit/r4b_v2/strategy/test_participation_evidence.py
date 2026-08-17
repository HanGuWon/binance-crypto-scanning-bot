from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from decimal import ROUND_FLOOR, Decimal, localcontext
from functools import cache

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import ROBUST_Z_PRIOR_WINDOW_V2
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBFeatureContractErrorV2,
    FamilyBFlowOnlyBarEvidenceV2,
    FamilyBFlowOnlyBarReadinessV2,
    FamilyBFlowWindowClosureV2,
    FamilyBNormalFlowTradeV2,
    build_family_b_flow_only_bar_evidence_v2,
    canonical_family_b_flow_only_bar_evidence_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    PARTICIPATION_FLOW_RULE_VERSION_V2,
    ParticipationFlowBarValueV2,
    ParticipationFlowCalculationV2,
    ParticipationFlowContractErrorV2,
    ParticipationFlowEvidenceV2,
    ParticipationFlowStatusV2,
    build_participation_flow_bar_value_v2,
    build_participation_flow_evidence_v2,
    calculate_participation_flow_v2,
    canonical_participation_flow_bar_value_v2,
    canonical_participation_flow_calculation_v2,
    canonical_participation_flow_evidence_v2,
)

ATTEMPT_ID = "participation-attempt"
SYMBOL = "BTCUSDT"
PLAN_SHA256 = "a" * 64
CAPTURE_ROOT_SHA256 = "b" * 64
SCHEMA_SHA256 = "c" * 64
BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _trade(
    *,
    bar_open_ms: int,
    trade_id: int,
    quantity: Decimal,
    normal_quantity: Decimal,
    buyer_maker: bool,
    receipt_ms: int | None = None,
    lineage_nonce: str = "baseline",
) -> FamilyBNormalFlowTradeV2:
    transaction_time_ms = bar_open_ms + trade_id * 1_000
    return FamilyBNormalFlowTradeV2.create(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        capture_root_sha256=CAPTURE_ROOT_SHA256,
        schema_sha256=SCHEMA_SHA256,
        trade_id=trade_id,
        transaction_time_ms=transaction_time_ms,
        receipt_ms=(transaction_time_ms + 1 if receipt_ms is None else receipt_ms),
        price=Decimal(1),
        quantity=quantity,
        normal_quantity=normal_quantity,
        contract_multiplier=Decimal(1),
        buyer_maker=buyer_maker,
        source_evidence_sha256=_sha(
            f"trade:{bar_open_ms}:{trade_id}:{quantity}:{normal_quantity}:"
            f"{buyer_maker}:{lineage_nonce}"
        ),
    )


def _closure(
    bar_open_ms: int,
    *,
    receipt_ms: int | None = None,
    lineage_nonce: str = "baseline",
) -> FamilyBFlowWindowClosureV2:
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    decision_cutoff_ms = bar_close_ms + DECISION_DELAY_MS_V2
    return FamilyBFlowWindowClosureV2.create(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        capture_root_sha256=CAPTURE_ROOT_SHA256,
        schema_sha256=SCHEMA_SHA256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        complete_through_event_ms=bar_close_ms,
        closure_event_ms=bar_close_ms + 1,
        closure_receipt_ms=(decision_cutoff_ms if receipt_ms is None else receipt_ms),
        source_evidence_sha256=_sha(f"closure:{bar_open_ms}:{lineage_nonce}"),
    )


def _flow_bar(
    bar_open_ms: int,
    *,
    signed_share: Decimal = Decimal("0.2"),
    total_notional: Decimal = Decimal(100),
    empty: bool = False,
    normal_quantity_zero: bool = False,
    reverse_trades: bool = False,
    closure_receipt_ms: int | None = None,
    lineage_nonce: str = "baseline",
) -> FamilyBFlowOnlyBarEvidenceV2:
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    decision_cutoff_ms = bar_close_ms + DECISION_DELAY_MS_V2
    trades: tuple[FamilyBNormalFlowTradeV2, ...]
    if empty:
        trades = ()
    else:
        with localcontext(protocol_decimal_context_v2()):
            buy_quantity = total_notional * (Decimal(1) + signed_share) / Decimal(2)
            sell_quantity = total_notional - buy_quantity
        buy_normal = Decimal(0) if normal_quantity_zero else buy_quantity
        sell_normal = Decimal(0) if normal_quantity_zero else sell_quantity
        trades = (
            _trade(
                bar_open_ms=bar_open_ms,
                trade_id=1,
                quantity=buy_quantity,
                normal_quantity=buy_normal,
                buyer_maker=False,
                lineage_nonce=lineage_nonce,
            ),
            _trade(
                bar_open_ms=bar_open_ms,
                trade_id=2,
                quantity=sell_quantity,
                normal_quantity=sell_normal,
                buyer_maker=True,
                lineage_nonce=lineage_nonce,
            ),
        )
        if reverse_trades:
            trades = tuple(reversed(trades))
    return build_family_b_flow_only_bar_evidence_v2(
        attempt_id=ATTEMPT_ID,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        normal_flow_capture_root_sha256=CAPTURE_ROOT_SHA256,
        normal_flow_nq_schema_sha256=SCHEMA_SHA256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        normal_flow_trades=trades,
        flow_window_closure=_closure(
            bar_open_ms,
            receipt_ms=closure_receipt_ms,
            lineage_nonce=lineage_nonce,
        ),
    )


@cache
def _prior_bars(
    *,
    zero_scale: bool = False,
    signed_share_center: Decimal = Decimal(0),
) -> tuple[FamilyBFlowOnlyBarEvidenceV2, ...]:
    first_open_ms = BAR_OPEN_MS - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    return tuple(
        _flow_bar(
            first_open_ms + index * FIVE_MINUTE_MS_V2,
            signed_share=(
                signed_share_center + Decimal("0.2")
                if zero_scale
                else signed_share_center + Decimal("0.2")
                if index % 2
                else signed_share_center - Decimal("0.2")
            ),
        )
        for index in range(ROBUST_Z_PRIOR_WINDOW_V2)
    )


def _participation(
    current: FamilyBFlowOnlyBarEvidenceV2,
    prior: tuple[FamilyBFlowOnlyBarEvidenceV2, ...] | None = None,
) -> ParticipationFlowEvidenceV2:
    return build_participation_flow_evidence_v2(
        attempt_id=ATTEMPT_ID,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=D_MS,
        current_bar=current,
        prior_bars=_prior_bars() if prior is None else prior,
    )


def _calculation_bar(value: FamilyBFlowOnlyBarEvidenceV2) -> ParticipationFlowBarValueV2:
    return build_participation_flow_bar_value_v2(
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        signed_normal_notional=value.signed_normal_notional,
        normal_notional=value.normal_notional,
        total_trade_notional=value.total_trade_notional,
        signed_share=value.signed_share,
    )


def test_flow_only_projection_is_directional_exact_and_excludes_price_features() -> None:
    positive = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("0.6"),
        total_notional=Decimal(50),
    )
    negative = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("-0.6"),
        total_notional=Decimal(50),
    )
    neutral = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal(0),
        total_notional=Decimal(50),
    )
    permuted = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("0.6"),
        total_notional=Decimal(50),
        reverse_trades=True,
    )

    assert positive.readiness is FamilyBFlowOnlyBarReadinessV2.READY
    assert positive.signed_normal_notional == Decimal(30)
    assert positive.normal_notional == Decimal(50)
    assert positive.total_trade_notional == Decimal(50)
    assert positive.flow_imbalance == Decimal("0.6")
    assert positive.signed_share == Decimal("0.6")
    assert positive.latest_source_event_ms == BAR_CLOSE_MS + 1
    assert positive.latest_source_receipt_ms == D_MS
    assert negative.signed_share == Decimal("-0.6")
    assert neutral.signed_share == 0
    assert permuted == positive
    payload = canonical_family_b_flow_only_bar_evidence_v2(positive)
    assert b"bar_return" not in payload
    assert b'"price"' not in payload
    assert b"book_" not in payload


def test_empty_and_zero_normal_quantity_are_inconclusive_without_numeric_fallback() -> None:
    empty = _flow_bar(BAR_OPEN_MS, empty=True)
    no_normal = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("0.6"),
        total_notional=Decimal(50),
        normal_quantity_zero=True,
    )

    for evidence in (empty, no_normal):
        assert evidence.readiness is FamilyBFlowOnlyBarReadinessV2.INCONCLUSIVE_DATA
        assert evidence.flow_imbalance is None
        assert evidence.signed_share is None
        participation = _participation(evidence)
        assert participation.status is ParticipationFlowStatusV2.INCONCLUSIVE_DATA
        assert participation.current_signed_share is None
        assert participation.direction == 0
        assert participation.strength_micros == 0


def test_exact_8640_rule_direction_activity_and_floor_strength() -> None:
    positive_current = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("0.6"),
        total_notional=Decimal(50),
    )
    positive = _participation(positive_current)
    negative = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=Decimal("-0.6"),
            total_notional=Decimal(50),
        )
    )
    neutral = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=Decimal(0),
            total_notional=Decimal(50),
        )
    )

    assert positive.status is ParticipationFlowStatusV2.READY
    assert positive.latest_source_event_ms == BAR_CLOSE_MS + 1
    assert positive.latest_source_receipt_ms == D_MS
    assert positive.prior_observation_count == 8_640
    assert positive.prior_signed_share_location == 0
    assert positive.prior_signed_share_mad == Decimal("0.2")
    assert positive.prior_signed_share_scale == Decimal("0.29652")
    assert positive.prior_total_notional_median == Decimal(100)
    assert positive.activity_support == Decimal("0.5")
    assert positive.direction == 1
    assert negative.direction == -1
    assert negative.strength_micros == positive.strength_micros
    assert neutral.direction == 0
    assert neutral.strength_micros == 0
    assert positive.current_projection_slice_sha256 != (positive_current.normal_flow_slice_sha256)
    assert positive.current_projection_slice_sha256 != (positive.prior_flow_slice_sha256)
    assert positive_current.normal_flow_slice_sha256.encode() in (
        canonical_family_b_flow_only_bar_evidence_v2(positive_current)
    )
    assert positive.shadow_only
    assert not positive.verified_raw_membership_m0_bound
    assert not positive.strict_source_parser_m1_bound
    assert not positive.causal_cursor_finality_m2_bound
    assert not positive.causal_inputs_complete
    assert "M0_ALONE_DOES_NOT_PROVE_CAUSAL_COMPLETENESS" in positive.reasons
    assert "STRICT_SOURCE_PARSER_M1_ABSENT" in positive.reasons
    assert "CAUSAL_CURSOR_FINALITY_M2_ABSENT" in positive.reasons
    assert "SHADOW_PROJECTION_NO_M0_M1_M2" in PARTICIPATION_FLOW_RULE_VERSION_V2
    with localcontext(protocol_decimal_context_v2()):
        expected_u = Decimal("0.6") / Decimal("0.29652")
        expected_strength = int(
            (
                Decimal(1_000_000)
                * (abs(expected_u) / (Decimal(1) + abs(expected_u)))
                * Decimal("0.5")
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
    assert positive.scaled_signed_share_u == expected_u
    assert positive.strength_micros == expected_strength
    canonical_participation_flow_evidence_v2(positive)


def test_nonzero_prior_location_does_not_center_absolute_flow_direction() -> None:
    current_share = Decimal("0.1")
    evidence = _participation(
        _flow_bar(BAR_OPEN_MS, signed_share=current_share),
        _prior_bars(signed_share_center=Decimal("0.2")),
    )

    assert evidence.status is ParticipationFlowStatusV2.READY
    location = evidence.prior_signed_share_location
    scale = evidence.prior_signed_share_scale
    scaled_u = evidence.scaled_signed_share_u
    assert location is not None
    assert location == Decimal("0.2")
    assert scale is not None
    assert scaled_u is not None
    with localcontext(protocol_decimal_context_v2()):
        expected_absolute_u = current_share / scale
        centered_robust_z = (current_share - location) / scale
    assert scaled_u == expected_absolute_u
    assert scaled_u != centered_robust_z
    assert scaled_u > 0
    assert centered_robust_z < 0
    assert evidence.direction == 1


def test_activity_support_caps_at_one_above_prior_median() -> None:
    at_median = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=Decimal("0.6"),
            total_notional=Decimal(100),
        )
    )
    above_median = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=Decimal("0.6"),
            total_notional=Decimal(1_000),
        )
    )

    assert above_median.prior_total_notional_median == Decimal(100)
    assert above_median.current_total_trade_notional == Decimal(1_000)
    assert at_median.activity_support == Decimal(1)
    assert above_median.activity_support == Decimal(1)
    assert above_median.strength_micros == at_median.strength_micros


def test_prior_permutation_is_canonical_but_gap_duplicate_and_oversize_fail() -> None:
    prior = _prior_bars()
    current = _flow_bar(BAR_OPEN_MS, signed_share=Decimal("0.6"))
    baseline = _participation(current, prior)
    permuted = _participation(current, tuple(reversed(prior)))
    changed_last = _flow_bar(
        prior[-1].bar_open_ms,
        signed_share=Decimal("0.3"),
    )
    changed = _participation(current, (*prior[:-1], changed_last))

    assert permuted == baseline
    assert changed.current_projection_slice_sha256 == (baseline.current_projection_slice_sha256)
    assert changed.prior_flow_slice_sha256 != baseline.prior_flow_slice_sha256
    assert changed.feature_slice_sha256 != baseline.feature_slice_sha256
    assert changed.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    with pytest.raises(ParticipationFlowContractErrorV2, match="contiguous"):
        _participation(current, (*prior[:100], *prior[101:]))
    with pytest.raises(ParticipationFlowContractErrorV2, match="unique"):
        _participation(current, (prior[0], prior[0], *prior[2:]))
    with pytest.raises(ParticipationFlowContractErrorV2, match="exceed"):
        _participation(current, (*prior, prior[-1]))


def test_economic_roots_ignore_source_lineage_but_evidence_seals_both() -> None:
    prior = _prior_bars()
    current = _flow_bar(BAR_OPEN_MS, signed_share=Decimal("0.6"))
    baseline = _participation(current, prior)
    relined_current = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=Decimal("0.6"),
            lineage_nonce="alternate-current-source",
        ),
        prior,
    )
    relined_prior = _participation(
        current,
        (
            *prior[:-1],
            _flow_bar(
                prior[-1].bar_open_ms,
                signed_share=Decimal("0.2"),
                lineage_nonce="alternate-prior-source",
            ),
        ),
    )

    for relined in (relined_current, relined_prior):
        assert relined.current_projection_slice_sha256 == (baseline.current_projection_slice_sha256)
        assert relined.prior_flow_slice_sha256 == baseline.prior_flow_slice_sha256
        assert relined.feature_slice_sha256 == baseline.feature_slice_sha256
        assert relined.direction == baseline.direction
        assert relined.strength_micros == baseline.strength_micros
        assert relined.source_lineage_root_sha256 != (baseline.source_lineage_root_sha256)
        assert relined.evidence_sha256 != baseline.evidence_sha256


def test_warmup_and_zero_scale_fail_closed_without_numeric_output() -> None:
    current = _flow_bar(BAR_OPEN_MS, signed_share=Decimal("0.6"))
    warmup = _participation(current, _prior_bars()[1:])
    zero = _participation(current, _prior_bars(zero_scale=True))

    assert warmup.status is ParticipationFlowStatusV2.FEATURE_NOT_READY_WARMUP
    assert warmup.prior_observation_count == 8_639
    assert warmup.current_signed_share is None
    assert warmup.strength_micros == 0
    assert zero.status is ParticipationFlowStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert zero.prior_signed_share_scale is None
    assert zero.direction == 0


def test_shared_participation_calculation_matches_shadow_evidence_and_is_sealed() -> None:
    current = _flow_bar(
        BAR_OPEN_MS,
        signed_share=Decimal("0.6"),
        total_notional=Decimal(50),
    )
    evidence = _participation(current)
    calculation = calculate_participation_flow_v2(
        current_bar=_calculation_bar(current),
        prior_bars=tuple(_calculation_bar(value) for value in _prior_bars()),
    )

    assert calculation.status is ParticipationFlowStatusV2.READY
    assert calculation.current_signed_share == evidence.current_signed_share
    assert calculation.prior_signed_share_scale == evidence.prior_signed_share_scale
    assert calculation.scaled_signed_share_u == evidence.scaled_signed_share_u
    assert calculation.activity_support == evidence.activity_support
    assert calculation.direction == evidence.direction
    assert calculation.strength_micros == evidence.strength_micros
    assert canonical_participation_flow_calculation_v2(calculation)
    assert canonical_participation_flow_bar_value_v2(_calculation_bar(current))

    calculation_values = {
        item.name: getattr(calculation, item.name) for item in fields(calculation) if item.init
    }
    with pytest.raises(ParticipationFlowContractErrorV2, match="frozen factory"):
        ParticipationFlowCalculationV2(**calculation_values)  # type: ignore[arg-type]
    with pytest.raises(ParticipationFlowContractErrorV2, match="frozen factory"):
        replace(calculation, strength_micros=0)


def test_shared_bar_value_rejects_contradictory_signed_share() -> None:
    with pytest.raises(ParticipationFlowContractErrorV2, match="contradicts"):
        build_participation_flow_bar_value_v2(
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            signed_normal_notional=Decimal(5),
            normal_notional=Decimal(5),
            total_trade_notional=Decimal(10),
            signed_share=Decimal("0.4"),
        )


def test_cutoff_and_trade_duplicate_fail_at_the_flow_owner_boundary() -> None:
    with pytest.raises(FamilyBFeatureContractErrorV2, match="after D"):
        _flow_bar(BAR_OPEN_MS, closure_receipt_ms=D_MS + 1)

    trade = _trade(
        bar_open_ms=BAR_OPEN_MS,
        trade_id=1,
        quantity=Decimal(10),
        normal_quantity=Decimal(10),
        buyer_maker=False,
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="duplicate"):
        build_family_b_flow_only_bar_evidence_v2(
            attempt_id=ATTEMPT_ID,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN_SHA256,
            normal_flow_capture_root_sha256=CAPTURE_ROOT_SHA256,
            normal_flow_nq_schema_sha256=SCHEMA_SHA256,
            bar_open_ms=BAR_OPEN_MS,
            bar_close_ms=BAR_CLOSE_MS,
            decision_cutoff_ms=D_MS,
            normal_flow_trades=(trade, trade),
            flow_window_closure=_closure(BAR_OPEN_MS),
        )


@pytest.mark.parametrize(
    "signed_share",
    (Decimal("1E-10"), Decimal("-1E-10")),
)
def test_tiny_nonzero_share_quantizes_to_ready_neutral_not_directional_zero_strength(
    signed_share: Decimal,
) -> None:
    tiny = _participation(
        _flow_bar(
            BAR_OPEN_MS,
            signed_share=signed_share,
            total_notional=Decimal("1E-20"),
        )
    )

    assert tiny.status is ParticipationFlowStatusV2.READY
    assert tiny.current_signed_share is not None
    assert (tiny.current_signed_share > 0) is (signed_share > 0)
    assert tiny.strength_micros == 0
    assert tiny.direction == 0


def test_direct_construction_and_replace_cannot_bypass_either_factory() -> None:
    flow_bar = _flow_bar(BAR_OPEN_MS, signed_share=Decimal("0.6"))
    participation = _participation(flow_bar)
    flow_values = {
        item.name: getattr(flow_bar, item.name) for item in fields(flow_bar) if item.init
    }
    participation_values = {
        item.name: getattr(participation, item.name) for item in fields(participation) if item.init
    }

    with pytest.raises(FamilyBFeatureContractErrorV2, match="causal factory"):
        FamilyBFlowOnlyBarEvidenceV2(**flow_values)  # type: ignore[arg-type]
    with pytest.raises(FamilyBFeatureContractErrorV2, match="causal factory"):
        replace(flow_bar, signed_normal_notional=Decimal(999))
    with pytest.raises(ParticipationFlowContractErrorV2, match="shadow factory"):
        ParticipationFlowEvidenceV2(**participation_values)  # type: ignore[arg-type]
    with pytest.raises(ParticipationFlowContractErrorV2, match="shadow factory"):
        replace(participation, strength_micros=999)


def test_canonical_serializers_reject_object_setattr_tampering() -> None:
    flow_bar = _flow_bar(BAR_OPEN_MS, signed_share=Decimal("0.6"))
    participation = _participation(flow_bar)

    object.__setattr__(flow_bar, "signed_share", Decimal("-0.6"))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="canonical content"):
        canonical_family_b_flow_only_bar_evidence_v2(flow_bar)

    object.__setattr__(
        participation,
        "strength_micros",
        participation.strength_micros + 1,
    )
    with pytest.raises(ParticipationFlowContractErrorV2, match="canonical content"):
        canonical_participation_flow_evidence_v2(participation)
