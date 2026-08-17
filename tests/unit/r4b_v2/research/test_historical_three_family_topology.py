from __future__ import annotations

import copy
import hashlib
import itertools
import json
from dataclasses import replace
from decimal import Decimal
from functools import cache
from typing import Any, cast

import pytest

from signalbot.domain.enums import Direction, SignalFamily
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import ROBUST_Z_PRIOR_WINDOW_V2
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
    HistoricalThreeFamilyComparisonBucketV2,
    HistoricalThreeFamilyDisplayGradeV2,
    HistoricalThreeFamilyMajorityDirectionV2,
    HistoricalThreeFamilyTopologyClassV2,
    HistoricalThreeFamilyTopologyErrorV2,
    canonical_historical_three_family_topology_v2,
    derive_historical_three_family_topology_v2,
)
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HistoricalCrossSectional7AssetCalculationV2,
    calculate_historical_cross_sectional_7asset_returns_v2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
    HistoricalDirectionalLeafV2,
    HistoricalFamilyV2,
    HistoricalLeafStatusV2,
    HistoricalRecommendationAnchorV2,
    HistoricalThreeFamilyConsensusV2,
    build_historical_directional_leaf_from_calculation_v2,
    build_historical_execution_contract_v2,
    build_historical_three_family_consensus_from_leaves_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    ParticipationFlowBarValueV2,
    ParticipationFlowCalculationV2,
    build_participation_flow_bar_value_v2,
    calculate_participation_flow_v2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PriceClosePathCalculationV2,
    calculate_price_return_series_v2,
)

TARGET = "OPUSDT"
ASSET = "OP"
FINAL_BAR_OPEN_MS = 2_000_160_000_000
FINAL_BAR_CLOSE_MS = FINAL_BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
SOURCE_EVENT_ID = "d" * 24
SOURCE_ROW_SHA256 = "b" * 64
REPLAY_MANIFEST_SHA256 = "c" * 64
EXPERIMENT_CONTRACT_SHA256 = "e" * 64
PEERS = tuple(
    sorted(
        (
            symbol
            for symbol in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2
            if symbol != TARGET
        ),
        key=lambda value: value.encode("utf-8"),
    )
)


def _anchor(direction: Direction) -> HistoricalRecommendationAnchorV2:
    return HistoricalRecommendationAnchorV2(
        source_event_id=SOURCE_EVENT_ID,
        source_row_sha256=SOURCE_ROW_SHA256,
        source_replay_manifest_sha256=REPLAY_MANIFEST_SHA256,
        split="development",
        asset=ASSET,
        cohort="volatile",
        symbol=TARGET,
        primary_family=(
            SignalFamily.PULLBACK_LONG
            if direction is Direction.LONG
            else SignalFamily.PULLBACK_SHORT
        ),
        primary_direction=direction,
        decision_time_ms=FINAL_BAR_CLOSE_MS,
        price=Decimal("100"),
        invalidation=Decimal("99") if direction is Direction.LONG else Decimal("101"),
        atr=Decimal("0.5"),
        source_rule_version=HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
        source_protocol_version=HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    )


@cache
def _prior_price_returns() -> tuple[Decimal, ...]:
    return tuple(
        Decimal((index % 7) - 3) / Decimal(1_000)
        for index in range(ROBUST_Z_PRIOR_WINDOW_V2)
    )


@cache
def _price_calculation(
    direction: int,
    magnitude: str = "0.01",
) -> PriceClosePathCalculationV2:
    current = Decimal(magnitude) * Decimal(direction)
    series = (*_prior_price_returns(), current)
    calculation = calculate_price_return_series_v2(series, series)
    assert calculation.direction == direction
    return calculation


@cache
def _participation_prior() -> tuple[ParticipationFlowBarValueV2, ...]:
    first_open_ms = FINAL_BAR_OPEN_MS - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    rows: list[ParticipationFlowBarValueV2] = []
    for index in range(ROBUST_Z_PRIOR_WINDOW_V2):
        share = Decimal((index % 7) - 3) / Decimal(10)
        rows.append(
            build_participation_flow_bar_value_v2(
                bar_open_ms=first_open_ms + index * FIVE_MINUTE_MS_V2,
                bar_close_ms=(
                    first_open_ms + (index + 1) * FIVE_MINUTE_MS_V2 - 1
                ),
                signed_normal_notional=share * Decimal(100),
                normal_notional=Decimal(100),
                total_trade_notional=Decimal(100),
                signed_share=share,
            )
        )
    return tuple(rows)


def _participation_bar(share: Decimal) -> ParticipationFlowBarValueV2:
    return build_participation_flow_bar_value_v2(
        bar_open_ms=FINAL_BAR_OPEN_MS,
        bar_close_ms=FINAL_BAR_CLOSE_MS,
        signed_normal_notional=share * Decimal(100),
        normal_notional=Decimal(100),
        total_trade_notional=Decimal(100),
        signed_share=share,
    )


@cache
def _participation_calculation(
    direction: int,
    magnitude: str = "0.5",
) -> ParticipationFlowCalculationV2:
    calculation = calculate_participation_flow_v2(
        current_bar=_participation_bar(Decimal(magnitude) * Decimal(direction)),
        prior_bars=_participation_prior(),
    )
    assert calculation.direction == direction
    return calculation


@cache
def _prior_cross_returns() -> tuple[Decimal, ...]:
    return tuple(
        Decimal((index % 7) - 3) / Decimal(1_000)
        for index in range(ROBUST_Z_PRIOR_WINDOW_V2)
    )


@cache
def _cross_calculation(
    direction: int,
    magnitude: str = "0.01",
) -> HistoricalCrossSectional7AssetCalculationV2:
    if direction == 0:
        current = tuple(
            Decimal(value)
            for value in ("-0.003", "-0.002", "-0.001", "0.001", "0.002", "0.003")
        )
    else:
        current = (Decimal(magnitude) * Decimal(direction),) * 6
    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=_prior_cross_returns(),
        current_peer_returns_3=current,
    )
    assert calculation.direction == direction
    return calculation


def _source_slice(family: HistoricalFamilyV2, suffix: str) -> str:
    return hashlib.sha256(f"{family.value}:{suffix}".encode()).hexdigest()


def _leaf_from_calculation(
    calculation: (
        PriceClosePathCalculationV2
        | ParticipationFlowCalculationV2
        | HistoricalCrossSectional7AssetCalculationV2
    ),
    *,
    suffix: str,
) -> HistoricalDirectionalLeafV2:
    family = {
        PriceClosePathCalculationV2: HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM,
        ParticipationFlowCalculationV2: HistoricalFamilyV2.PARTICIPATION_FLOW,
        HistoricalCrossSectional7AssetCalculationV2: (
            HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET
        ),
    }[type(calculation)]
    return build_historical_directional_leaf_from_calculation_v2(
        calculation=calculation,
        symbol=TARGET,
        venue=VenueV2.USDM_FUTURES,
        interval="5m",
        bar_open_ms=FINAL_BAR_OPEN_MS,
        bar_close_ms=FINAL_BAR_CLOSE_MS,
        historical_slice_through_ms=FINAL_BAR_CLOSE_MS,
        source_slice_sha256=_source_slice(family, suffix),
    )


@cache
def _ready_leaf(family: HistoricalFamilyV2, direction: int) -> HistoricalDirectionalLeafV2:
    calculation = {
        HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM: _price_calculation(direction),
        HistoricalFamilyV2.PARTICIPATION_FLOW: _participation_calculation(direction),
        HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET: _cross_calculation(direction),
    }[family]
    return _leaf_from_calculation(calculation, suffix=f"ready:{direction}")


@cache
def _nonready_leaf(family: HistoricalFamilyV2) -> HistoricalDirectionalLeafV2:
    if family is HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM:
        zeros = (Decimal(0),) * (ROBUST_Z_PRIOR_WINDOW_V2 + 1)
        calculation: (
            PriceClosePathCalculationV2
            | ParticipationFlowCalculationV2
            | HistoricalCrossSectional7AssetCalculationV2
        ) = calculate_price_return_series_v2(zeros, zeros)
    elif family is HistoricalFamilyV2.PARTICIPATION_FLOW:
        calculation = calculate_participation_flow_v2(
            current_bar=_participation_bar(Decimal("0.5")),
            prior_bars=(),
        )
    else:
        calculation = calculate_historical_cross_sectional_7asset_returns_v2(
            prior_market_median_returns_3=(Decimal(0),) * ROBUST_Z_PRIOR_WINDOW_V2,
            current_peer_returns_3=(Decimal("0.1"),) * 6,
        )
    leaf = _leaf_from_calculation(calculation, suffix="nonready")
    assert leaf.status is not HistoricalLeafStatusV2.READY
    return leaf


def _consensus(
    directions: tuple[int, int, int],
    *,
    primary_direction: Direction = Direction.LONG,
    leaves: tuple[HistoricalDirectionalLeafV2, ...] | None = None,
) -> HistoricalThreeFamilyConsensusV2:
    exact_leaves = leaves or tuple(
        _ready_leaf(family, direction)
        for family, direction in zip(HistoricalFamilyV2, directions, strict=True)
    )
    cross_leaf = next(
        leaf
        for leaf in exact_leaves
        if leaf.family is HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET
    )
    peer_paths = tuple(
        (symbol, hashlib.sha256(f"peer:{symbol}".encode()).hexdigest())
        for symbol in PEERS
    )
    return build_historical_three_family_consensus_from_leaves_v2(
        anchor=_anchor(primary_direction),
        leaves=exact_leaves,
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
        cross_peer_path_sha256s=peer_paths,
        cross_peer_input_sha256=cross_leaf.source_slice_sha256,
    )


def _expected_topology(
    directions: tuple[int, int, int],
) -> HistoricalThreeFamilyTopologyClassV2:
    counts = (directions.count(1), directions.count(-1), directions.count(0))
    return {
        (3, 0, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
        (0, 3, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
        (2, 0, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
        (0, 2, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
        (2, 1, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0,
        (1, 2, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0,
        (1, 0, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2,
        (0, 1, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2,
        (1, 1, 1): HistoricalThreeFamilyTopologyClassV2.BALANCED_1_1_1,
        (0, 0, 3): HistoricalThreeFamilyTopologyClassV2.ALL_NEUTRAL_0_0_3,
    }[counts]


def _expected_labels(
    topology: HistoricalThreeFamilyTopologyClassV2,
) -> tuple[
    HistoricalThreeFamilyComparisonBucketV2,
    HistoricalThreeFamilyDisplayGradeV2,
]:
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilyDisplayGradeV2.UNANIMOUS_BREADTH_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.CLEAN_2_PLUS_NEUTRAL,
            HistoricalThreeFamilyDisplayGradeV2.CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0,
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1,
            HistoricalThreeFamilyDisplayGradeV2.CONFLICTED_MAJORITY_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2,
        HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.NOT_COMPARABLE,
            HistoricalThreeFamilyDisplayGradeV2.INSUFFICIENT_DIRECTIONAL_BREADTH,
        )
    return (
        HistoricalThreeFamilyComparisonBucketV2.NOT_COMPARABLE,
        HistoricalThreeFamilyDisplayGradeV2.NO_DIRECTIONAL_CONSENSUS,
    )


@pytest.mark.parametrize("directions", tuple(itertools.product((-1, 0, 1), repeat=3)))
def test_all_27_ready_sign_tuples_have_one_exact_topology_and_admission_parity(
    directions: tuple[int, int, int],
) -> None:
    for primary_direction in (Direction.LONG, Direction.SHORT):
        consensus = _consensus(directions, primary_direction=primary_direction)
        value = derive_historical_three_family_topology_v2(consensus)
        expected_topology = _expected_topology(directions)
        expected_bucket, expected_grade = _expected_labels(expected_topology)
        bullish = directions.count(1)
        bearish = directions.count(-1)
        neutral = directions.count(0)
        support = bullish if primary_direction is Direction.LONG else bearish
        oppose = bearish if primary_direction is Direction.LONG else bullish

        assert value.topology is expected_topology
        assert value.comparison_bucket is expected_bucket
        assert value.display_grade is expected_grade
        assert value.ready_family_count == 3
        assert value.unavailable_family_count == 0
        assert value.bullish_family_count == bullish
        assert value.bearish_family_count == bearish
        assert value.neutral_family_count == neutral
        assert value.has_opposition is (bullish > 0 and bearish > 0)
        assert value.primary_support_count == support
        assert value.primary_oppose_count == oppose
        assert value.primary_neutral_count == neutral
        assert value.clean_primary_audit_eligible is consensus.admitted
        assert value.conflicted_comparator_eligible is (
            expected_bucket
            is HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1
            and support == 2
            and oppose == 1
        )
        if bullish >= 2:
            assert value.majority_direction is (
                HistoricalThreeFamilyMajorityDirectionV2.BULLISH
            )
            assert value.majority_family_count == bullish
            assert value.opposing_family_count == bearish
        elif bearish >= 2:
            assert value.majority_direction is (
                HistoricalThreeFamilyMajorityDirectionV2.BEARISH
            )
            assert value.majority_family_count == bearish
            assert value.opposing_family_count == bullish
        else:
            assert value.majority_direction is None
            assert value.majority_family_count is None
            assert value.opposing_family_count is None
        assert value.outcome_data_used is False
        assert value.probability is False
        assert value.probability_calibrated is False
        assert value.promoting is False
        assert value.changes_consensus_decision is False


@pytest.mark.parametrize("unavailable_family", tuple(HistoricalFamilyV2))
def test_any_nonready_leaf_is_withheld_and_unknown_not_neutral(
    unavailable_family: HistoricalFamilyV2,
) -> None:
    leaves = tuple(
        _nonready_leaf(family)
        if family is unavailable_family
        else _ready_leaf(family, 1)
        for family in HistoricalFamilyV2
    )
    value = derive_historical_three_family_topology_v2(
        _consensus((1, 1, 1), leaves=leaves)
    )

    assert value.topology is HistoricalThreeFamilyTopologyClassV2.WITHHELD
    assert value.comparison_bucket is HistoricalThreeFamilyComparisonBucketV2.WITHHELD
    assert value.display_grade is HistoricalThreeFamilyDisplayGradeV2.WITHHELD_DATA
    assert value.ready_family_count == 2
    assert value.unavailable_family_count == 1
    assert value.bullish_family_count is None
    assert value.bearish_family_count is None
    assert value.neutral_family_count is None
    assert value.primary_support_count is None
    assert value.clean_primary_audit_eligible is False


def test_primary_flip_changes_only_relative_semantics_not_absolute_topology() -> None:
    directions = (1, 1, 0)
    bullish_primary = derive_historical_three_family_topology_v2(
        _consensus(directions, primary_direction=Direction.LONG)
    )
    bearish_primary = derive_historical_three_family_topology_v2(
        _consensus(directions, primary_direction=Direction.SHORT)
    )

    assert bullish_primary.topology is bearish_primary.topology
    assert bullish_primary.comparison_bucket is bearish_primary.comparison_bucket
    assert bullish_primary.majority_direction is bearish_primary.majority_direction
    assert bullish_primary.primary_support_count == 2
    assert bullish_primary.primary_oppose_count == 0
    assert bullish_primary.clean_primary_audit_eligible is True
    assert bearish_primary.primary_support_count == 0
    assert bearish_primary.primary_oppose_count == 2
    assert bearish_primary.clean_primary_audit_eligible is False
    assert bullish_primary.topology_sha256 != bearish_primary.topology_sha256


def test_opposing_magnitude_cannot_turn_two_vs_one_into_clean_agreement() -> None:
    price = _leaf_from_calculation(
        _price_calculation(1, "0.0000001"),
        suffix="weak-positive",
    )
    participation = _leaf_from_calculation(
        _participation_calculation(1, "0.00001"),
        suffix="weak-positive",
    )
    cross = _leaf_from_calculation(
        _cross_calculation(-1, "100"),
        suffix="strong-negative",
    )
    consensus = _consensus((1, 1, -1), leaves=(price, participation, cross))
    value = derive_historical_three_family_topology_v2(consensus)

    assert consensus.directional_numerator_micros is not None
    assert consensus.directional_numerator_micros < 0
    assert value.topology is (
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0
    )
    assert value.comparison_bucket is (
        HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1
    )
    assert value.majority_direction is HistoricalThreeFamilyMajorityDirectionV2.BULLISH
    assert value.majority_family_count == 2
    assert value.opposing_family_count == 1
    assert value.has_opposition is True
    assert value.clean_primary_audit_eligible is False
    assert value.conflicted_comparator_eligible is True
    assert value.conflicted_comparator_outcome_authorized is False


def test_cross_nonzero_sign_at_zero_strength_remains_a_sign_not_a_neutral() -> None:
    tiny_cross_calculation = _cross_calculation(1, "1E-40")
    assert tiny_cross_calculation.strength_micros == 0
    cross = _leaf_from_calculation(tiny_cross_calculation, suffix="tiny-positive")
    leaves = (
        _ready_leaf(HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM, 0),
        _ready_leaf(HistoricalFamilyV2.PARTICIPATION_FLOW, 0),
        cross,
    )
    value = derive_historical_three_family_topology_v2(
        _consensus((0, 0, 1), leaves=leaves)
    )

    assert cross.direction == 1
    assert cross.strength_micros == 0
    assert value.topology is HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2
    assert value.bullish_family_count == 1
    assert value.neutral_family_count == 2
    assert value.display_grade is (
        HistoricalThreeFamilyDisplayGradeV2.INSUFFICIENT_DIRECTIONAL_BREADTH
    )


def test_canonical_hash_is_deterministic_bound_to_source_and_fixed_false_claims() -> None:
    consensus = _consensus((1, 1, 1))
    first = derive_historical_three_family_topology_v2(consensus)
    second = derive_historical_three_family_topology_v2(consensus)
    first_bytes = canonical_historical_three_family_topology_v2(first)

    assert first == second
    assert first_bytes == canonical_historical_three_family_topology_v2(second)
    assert first.rule_version == HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2
    document = json.loads(first_bytes)
    assert document["source_consensus_event_id"] == consensus.event_id
    assert document["source_consensus_payload_sha256"] == consensus.payload_sha256
    assert document["source_anchor_sha256"] == consensus.anchor.anchor_sha256
    assert document["topology_sha256"] == first.topology_sha256
    assert document["probability"] is False
    assert document["probability_calibrated"] is False
    assert document["promoting"] is False
    assert document["order_instruction"] is False
    assert document["outcome_data_used"] is False
    assert len(document["leaf_states"]) == 3


def test_factory_and_canonical_boundaries_fail_closed_on_tampering() -> None:
    value = derive_historical_three_family_topology_v2(_consensus((1, 1, 1)))
    with pytest.raises(
        HistoricalThreeFamilyTopologyErrorV2,
        match="require their frozen derivation factory",
    ):
        replace(value, display_grade=HistoricalThreeFamilyDisplayGradeV2.WITHHELD_DATA)

    object.__setattr__(value, "topology_sha256", "0" * 64)
    with pytest.raises(HistoricalThreeFamilyTopologyErrorV2, match="topology hash differs"):
        canonical_historical_three_family_topology_v2(value)


def test_invalid_source_type_and_tampered_source_consensus_fail_closed() -> None:
    with pytest.raises(
        HistoricalThreeFamilyTopologyErrorV2,
        match="source_consensus must be an exact",
    ):
        derive_historical_three_family_topology_v2("not-a-consensus")  # type: ignore[arg-type]

    changed_admission = copy.deepcopy(_consensus((1, 1, 1)))
    object.__setattr__(changed_admission, "admitted", False)
    with pytest.raises(
        HistoricalThreeFamilyTopologyErrorV2,
        match="source consensus failed canonical validation",
    ):
        derive_historical_three_family_topology_v2(changed_admission)

    duplicate_family = copy.deepcopy(_consensus((1, 1, 1)))
    object.__setattr__(
        duplicate_family,
        "leaves",
        (duplicate_family.leaves[0],) * 3,
    )
    with pytest.raises(
        HistoricalThreeFamilyTopologyErrorV2,
        match="source consensus failed canonical validation",
    ):
        derive_historical_three_family_topology_v2(duplicate_family)


def test_canonical_function_rejects_non_topology_values() -> None:
    with pytest.raises(
        HistoricalThreeFamilyTopologyErrorV2,
        match="exact HistoricalThreeFamilyTopologyV2",
    ):
        canonical_historical_three_family_topology_v2(
            cast(Any, _consensus((1, 1, 1)))
        )
