from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import replace
from decimal import Decimal, localcontext
from functools import cache

import pytest

from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HistoricalCrossSectional7AssetCalculationV2,
    HistoricalCrossSectional7AssetProxyInputV2,
    HistoricalCrossSectional7AssetProxyV2,
    HistoricalPeerCandlePathV2,
    build_historical_cross_sectional_7asset_proxy_v2,
    calculate_historical_cross_sectional_7asset_returns_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FamilyCClosedCandleV2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
    HistoricalConsensusRegistryDispositionV2,
    HistoricalConsensusStatusV2,
    HistoricalCostSurvivalStatusV2,
    HistoricalDirectionalLeafV2,
    HistoricalFamilyV2,
    HistoricalLeafSourceRepresentationV2,
    HistoricalLeafStatusV2,
    HistoricalRecommendationAnchorV2,
    HistoricalThreeFamilyConsensusContractErrorV2,
    HistoricalThreeFamilyConsensusRegistryV2,
    HistoricalThreeFamilyConsensusV2,
    PrimaryRelationshipV2,
    build_historical_cost_survival_context_v2,
    build_historical_directional_leaf_from_calculation_v2,
    build_historical_directional_leaf_v2,
    build_historical_execution_contract_v2,
    build_historical_three_family_consensus_from_leaves_v2,
    build_historical_three_family_consensus_v2,
    calculate_historical_three_family_aggregation_v2,
    canonical_historical_three_family_consensus_v2,
    resolve_historical_consensus_status_v2,
)
from signalbot.r4b_v2.strategy.participation_historical_kline_proxy import (
    PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2,
    ParticipationHistoricalKlineProxyV2,
    build_participation_historical_kline_proxy_v2,
)
from signalbot.r4b_v2.strategy.price_historical_kline_proxy import (
    PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2,
    PriceHistoricalKlineProxyV2,
    build_price_historical_kline_proxy_v2,
)

TARGET = "OPUSDT"
ASSET = "OP"
FINAL_BAR_OPEN_MS = 2_000_160_000_000
FINAL_BAR_CLOSE_MS = FINAL_BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DATASET_SHA256 = "a" * 64
EXPERIMENT_CONTRACT_SHA256 = "e" * 64
SOURCE_ROW_SHA256 = "b" * 64
REPLAY_MANIFEST_SHA256 = "c" * 64
SOURCE_EVENT_ID = "d" * 24
PEERS = tuple(
    sorted(
        (symbol for symbol in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2 if symbol != TARGET),
        key=lambda value: value.encode("utf-8"),
    )
)


def _anchor(
    *,
    direction: Direction = Direction.LONG,
    source_row_sha256: str = SOURCE_ROW_SHA256,
    invalidation: Decimal | None = Decimal("99"),
    symbol: str = TARGET,
    asset: str = ASSET,
) -> HistoricalRecommendationAnchorV2:
    return HistoricalRecommendationAnchorV2(
        source_event_id=SOURCE_EVENT_ID,
        source_row_sha256=source_row_sha256,
        source_replay_manifest_sha256=REPLAY_MANIFEST_SHA256,
        split="development",
        asset=asset,
        cohort="volatile",
        symbol=symbol,
        primary_family=(
            SignalFamily.PULLBACK_LONG
            if direction is Direction.LONG
            else SignalFamily.PULLBACK_SHORT
        ),
        primary_direction=direction,
        decision_time_ms=FINAL_BAR_CLOSE_MS,
        price=Decimal("100"),
        invalidation=invalidation,
        atr=Decimal("0.50"),
        source_rule_version=HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
        source_protocol_version=HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    )


def _candle(
    *,
    open_time_ms: int,
    close: Decimal,
    quote_volume: Decimal,
    taker_buy_quote_volume: Decimal,
) -> Candle:
    return Candle(
        market=Market.FUTURES,
        symbol=TARGET,
        interval="5m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + FIVE_MINUTE_MS_V2 - 1,
        open=close,
        high=close + Decimal(1),
        low=close - Decimal(1),
        close=close,
        volume=Decimal(10),
        quote_volume=quote_volume,
        trade_count=100,
        taker_buy_base_volume=Decimal(4),
        taker_buy_quote_volume=taker_buy_quote_volume,
        is_closed=True,
    )


@cache
def _price_proxy() -> PriceHistoricalKlineProxyV2:
    first = FINAL_BAR_OPEN_MS - (
        (PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2 - 1)
        * FIVE_MINUTE_MS_V2
    )
    rows = tuple(
        _candle(
            open_time_ms=first + index * FIVE_MINUTE_MS_V2,
            close=(
                Decimal(100)
                + Decimal(index) * Decimal("0.001")
                + Decimal(index % 7) * Decimal("0.0001")
            ),
            quote_volume=Decimal(1_000 + index % 17),
            taker_buy_quote_volume=Decimal(400),
        )
        for index in range(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)
    )
    return build_price_historical_kline_proxy_v2(
        attempt_id="historical-consensus-price",
        dataset_sha256=DATASET_SHA256,
        bar_open_ms=FINAL_BAR_OPEN_MS,
        rows=rows,
    )


@cache
def _participation_proxy() -> ParticipationHistoricalKlineProxyV2:
    first = FINAL_BAR_OPEN_MS - (
        (PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2 - 1)
        * FIVE_MINUTE_MS_V2
    )
    rows: list[Candle] = []
    for index in range(PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2):
        quote = Decimal(1_000 + index % 17)
        fraction = Decimal("0.40") + Decimal(index % 7) * Decimal("0.01")
        if index == PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2 - 1:
            fraction = Decimal("0.80")
        rows.append(
            _candle(
                open_time_ms=first + index * FIVE_MINUTE_MS_V2,
                close=Decimal(100),
                quote_volume=quote,
                taker_buy_quote_volume=quote * fraction,
            )
        )
    return build_participation_historical_kline_proxy_v2(
        attempt_id="historical-consensus-participation",
        dataset_sha256=DATASET_SHA256,
        bar_open_ms=FINAL_BAR_OPEN_MS,
        rows=tuple(rows),
    )


def _path_hash(symbol: str, index: int) -> str:
    return hashlib.sha256(f"{symbol}:{index}".encode()).hexdigest()


@cache
def _peer_path(symbol: str) -> HistoricalPeerCandlePathV2:
    peer_index = PEERS.index(symbol)
    first = FINAL_BAR_OPEN_MS - (
        (FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
    )
    base = Decimal(100 + peer_index * 20)
    rows: list[FamilyCClosedCandleV2] = []
    for index in range(FAMILY_C_PANEL_BAR_COUNT_V2):
        open_ms = first + index * FIVE_MINUTE_MS_V2
        close = (
            base
            + Decimal(index) * Decimal("0.01")
            + Decimal(index % 17) * Decimal("0.001")
        )
        rows.append(
            FamilyCClosedCandleV2(
                symbol=symbol,
                bar_open_ms=open_ms,
                bar_close_ms=open_ms + FIVE_MINUTE_MS_V2 - 1,
                event_time_ms=open_ms + FIVE_MINUTE_MS_V2 - 1,
                receipt_time_ms=open_ms + FIVE_MINUTE_MS_V2 - 1,
                close=close,
                source_evidence_sha256=_path_hash(symbol, index),
            )
        )
    rows[-1] = replace(
        rows[-1],
        close=rows[-4].close
        * (Decimal(1) + Decimal(peer_index + 1) * Decimal("0.001")),
    )
    return HistoricalPeerCandlePathV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        candles=tuple(rows),
    )


@cache
def _cross_proxy() -> HistoricalCrossSectional7AssetProxyV2:
    return build_historical_cross_sectional_7asset_proxy_v2(
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=tuple(_peer_path(symbol) for symbol in PEERS),
        )
    )


@cache
def _cross_calculation() -> HistoricalCrossSectional7AssetCalculationV2:
    prior_rows: list[list[Decimal]] = [
        [] for _ in range(FAMILY_C_PANEL_BAR_COUNT_V2 - 4)
    ]
    current: list[Decimal] = []
    with localcontext(protocol_decimal_context_v2()):
        for symbol in PEERS:
            closes = tuple(candle.close for candle in _peer_path(symbol).candles)
            for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1):
                prior_rows[index - 3].append(
                    (closes[index] / closes[index - 3]).ln()
                )
            current.append((closes[-1] / closes[-4]).ln())
        prior_medians = tuple(
            (sorted(row)[2] + sorted(row)[3]) / Decimal(2) for row in prior_rows
        )
    return calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_medians,
        current_peer_returns_3=tuple(current),
    )


def _proxies() -> tuple[
    PriceHistoricalKlineProxyV2,
    ParticipationHistoricalKlineProxyV2,
    HistoricalCrossSectional7AssetProxyV2,
]:
    return (_price_proxy(), _participation_proxy(), _cross_proxy())


@cache
def _consensus() -> HistoricalThreeFamilyConsensusV2:
    return build_historical_three_family_consensus_v2(
        anchor=_anchor(),
        source_proxies=_proxies(),
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
    )


def _expected_state(directions: tuple[int, int, int]) -> DirectionalStateClassV2:
    bullish = directions.count(1)
    bearish = directions.count(-1)
    if bullish == 3 and bearish == 0:
        return DirectionalStateClassV2.BROAD_BULLISH_STATE
    if bullish >= 2 and bearish == 0:
        return DirectionalStateClassV2.BULLISH_STATE_TILT
    if bearish == 3 and bullish == 0:
        return DirectionalStateClassV2.BROAD_BEARISH_STATE
    if bearish >= 2 and bullish == 0:
        return DirectionalStateClassV2.BEARISH_STATE_TILT
    return DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE


@pytest.mark.parametrize("directions", tuple(itertools.product((-1, 0, 1), repeat=3)))
def test_all_27_direction_sign_combinations_match_frozen_state_parity(
    directions: tuple[int, int, int],
) -> None:
    magnitudes = (100_000, 200_000, 300_000)
    strengths = tuple(
        0 if direction == 0 else magnitude
        for direction, magnitude in zip(directions, magnitudes, strict=True)
    )
    result = calculate_historical_three_family_aggregation_v2(
        tuple(zip(directions, strengths, strict=True))
    )

    assert result.state_class is _expected_state(directions)
    assert result.bullish_family_count == directions.count(1)
    assert result.bearish_family_count == directions.count(-1)
    assert result.neutral_family_count == directions.count(0)
    assert result.directional_denominator == 3


def test_signed_strength_rounding_and_zero_strength_nonzero_cross_sign_are_preserved() -> None:
    positive = calculate_historical_three_family_aggregation_v2(
        ((1, 1), (1, 1), (0, 0))
    )
    negative = calculate_historical_three_family_aggregation_v2(
        ((-1, 1), (-1, 1), (0, 0))
    )
    quantized_cross = calculate_historical_three_family_aggregation_v2(
        ((0, 0), (0, 0), (1, 0))
    )

    assert positive.directional_numerator_micros == 2
    assert positive.directional_agreement_micros == 1
    assert negative.directional_numerator_micros == -2
    assert negative.directional_agreement_micros == -1
    assert quantized_cross.bullish_family_count == 1
    assert quantized_cross.directional_agreement_micros == 0
    with pytest.raises(
        HistoricalThreeFamilyConsensusContractErrorV2,
        match="neutral READY direction cannot carry",
    ):
        calculate_historical_three_family_aggregation_v2(
            ((0, 1), (0, 0), (1, 0))
        )


@pytest.mark.parametrize(
    ("statuses", "expected"),
    (
        (
            (
                HistoricalLeafStatusV2.FEATURE_NOT_READY,
                HistoricalLeafStatusV2.INCONCLUSIVE_DATA,
                HistoricalLeafStatusV2.DATA_INVALID,
            ),
            HistoricalConsensusStatusV2.DATA_INVALID,
        ),
        (
            (
                HistoricalLeafStatusV2.READY,
                HistoricalLeafStatusV2.FEATURE_NOT_READY,
                HistoricalLeafStatusV2.INCONCLUSIVE_DATA,
            ),
            HistoricalConsensusStatusV2.INCONCLUSIVE_DATA,
        ),
        (
            (
                HistoricalLeafStatusV2.READY,
                HistoricalLeafStatusV2.FEATURE_NOT_READY,
                HistoricalLeafStatusV2.READY,
            ),
            HistoricalConsensusStatusV2.FEATURE_NOT_READY,
        ),
        (
            (HistoricalLeafStatusV2.READY,) * 3,
            HistoricalConsensusStatusV2.READY,
        ),
    ),
)
def test_missing_status_precedence_is_fail_closed(
    statuses: tuple[HistoricalLeafStatusV2, ...],
    expected: HistoricalConsensusStatusV2,
) -> None:
    assert resolve_historical_consensus_status_v2(statuses) is expected


def test_anchor_execution_and_cost_context_are_exact_and_wrong_side_stop_is_retained() -> None:
    wrong_side = _anchor(invalidation=Decimal("101"))
    execution = build_historical_execution_contract_v2()
    cost = build_historical_cost_survival_context_v2(
        anchor=wrong_side,
        execution_contract=execution,
    )

    assert HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2 == (
        "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_CONSENSUS_V1_FROZEN"
    )
    assert wrong_side.invalidation == Decimal("101")
    assert execution.zero_move_round_trip_cost_micros == 2_600
    assert cost.status is HistoricalCostSurvivalStatusV2.AVAILABLE
    assert cost.atr_fraction_micros == 5_000
    assert cost.one_atr_cost_headroom_micros == 2_400
    assert cost.directional_vote is False
    assert cost.selection_gate_applied is False
    assert cost.funding_included is False


def test_exact_proxies_build_absolute_leaves_and_support_or_oppose_only_after_state() -> None:
    long_value = _consensus()
    assert hasattr(long_value, "leaves")
    assert long_value.state_class is DirectionalStateClassV2.BROAD_BULLISH_STATE
    assert long_value.primary_relationship is PrimaryRelationshipV2.SUPPORTS_PRIMARY
    assert long_value.admitted is True
    assert tuple(leaf.family for leaf in long_value.leaves) == tuple(HistoricalFamilyV2)
    assert all(leaf.primary_direction_used is False for leaf in long_value.leaves)

    short_value = build_historical_three_family_consensus_v2(
        anchor=_anchor(direction=Direction.SHORT, invalidation=Decimal("99")),
        source_proxies=tuple(reversed(_proxies())),
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
    )
    assert short_value.state_class is long_value.state_class
    assert short_value.directional_numerator_micros == long_value.directional_numerator_micros
    assert short_value.primary_relationship is PrimaryRelationshipV2.OPPOSES_PRIMARY
    assert short_value.admitted is False


def test_proxy_permutation_is_canonical_and_payload_binds_peer_root_and_false_claims() -> None:
    original = _consensus()
    permuted = build_historical_three_family_consensus_v2(
        anchor=_anchor(),
        source_proxies=tuple(reversed(_proxies())),
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
    )

    assert original == permuted
    document = json.loads(canonical_historical_three_family_consensus_v2(original))
    assert document["cross_peer_symbols"] == list(PEERS)
    assert len(document["cross_peer_set_root_sha256"]) == 64
    assert document["probability"] is False
    assert document["probability_calibrated"] is False
    assert document["promoting"] is False
    assert document["order_instruction"] is False
    assert document["order_placement"] is False
    assert document["outcome_data_used"] is False


def test_compact_calculation_leaves_are_byte_identical_to_exact_proxy_consensus() -> None:
    price, participation, cross = _proxies()
    assert participation.calculation is not None
    price_leaf = build_historical_directional_leaf_from_calculation_v2(
        calculation=price.calculation,
        symbol=TARGET,
        venue=VenueV2.USDM_FUTURES,
        interval="5m",
        bar_open_ms=FINAL_BAR_OPEN_MS,
        bar_close_ms=FINAL_BAR_CLOSE_MS,
        historical_slice_through_ms=FINAL_BAR_CLOSE_MS,
        source_slice_sha256=price.economic_close_slice_sha256,
    )
    participation_leaf = build_historical_directional_leaf_from_calculation_v2(
        calculation=participation.calculation,
        symbol=TARGET,
        venue=VenueV2.USDM_FUTURES,
        interval="5m",
        bar_open_ms=FINAL_BAR_OPEN_MS,
        bar_close_ms=FINAL_BAR_CLOSE_MS,
        historical_slice_through_ms=FINAL_BAR_CLOSE_MS,
        source_slice_sha256=participation.economic_flow_root_sha256,
    )
    cross_calculation = _cross_calculation()
    assert cross_calculation.calculation_sha256 == cross.calculation_sha256
    cross_leaf = build_historical_directional_leaf_from_calculation_v2(
        calculation=cross_calculation,
        symbol=TARGET,
        venue=VenueV2.USDM_FUTURES,
        interval="5m",
        bar_open_ms=FINAL_BAR_OPEN_MS,
        bar_close_ms=FINAL_BAR_CLOSE_MS,
        historical_slice_through_ms=FINAL_BAR_CLOSE_MS,
        source_slice_sha256=cross.source_input.input_sha256,
    )
    compact = build_historical_three_family_consensus_from_leaves_v2(
        anchor=_anchor(),
        leaves=(cross_leaf, price_leaf, participation_leaf),
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
        cross_peer_path_sha256s=tuple(
            reversed(
                tuple(
                    (path.symbol, path.path_sha256)
                    for path in cross.source_input.peer_paths
                )
            )
        ),
        cross_peer_input_sha256=cross.source_input.input_sha256,
    )
    exact = _consensus()

    assert compact == exact
    assert compact.event_id == exact.event_id
    assert compact.payload_sha256 == exact.payload_sha256
    assert canonical_historical_three_family_consensus_v2(
        compact
    ) == canonical_historical_three_family_consensus_v2(exact)
    assert all(
        leaf.source_representation
        is HistoricalLeafSourceRepresentationV2.CANONICAL_NUMERIC_CALCULATION
        for leaf in compact.leaves
    )


def test_stable_identity_excludes_evidence_hashes_but_registry_detects_payload_conflict() -> None:
    first = _consensus()
    changed = build_historical_three_family_consensus_v2(
        anchor=_anchor(source_row_sha256="f" * 64),
        source_proxies=_proxies(),
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
    )
    registry = HistoricalThreeFamilyConsensusRegistryV2(maximum_events=1)

    assert first.event_id == changed.event_id
    assert first.payload_sha256 != changed.payload_sha256
    assert registry.register(first) is HistoricalConsensusRegistryDispositionV2.NEW
    assert (
        registry.register(first)
        is HistoricalConsensusRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    with pytest.raises(
        HistoricalThreeFamilyConsensusContractErrorV2,
        match="collides with a different payload",
    ):
        registry.register(changed)


def test_factory_and_canonical_boundaries_reject_duplicates_scope_hash_and_direct_leaf() -> None:
    with pytest.raises(
        HistoricalThreeFamilyConsensusContractErrorV2,
        match="exactly one proxy per family",
    ):
        build_historical_three_family_consensus_v2(
            anchor=_anchor(),
            source_proxies=(_price_proxy(), _price_proxy(), _cross_proxy()),
            execution_contract=build_historical_execution_contract_v2(),
            experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
        )
    with pytest.raises(
        HistoricalThreeFamilyConsensusContractErrorV2,
        match="price proxy symbol differs",
    ):
        build_historical_three_family_consensus_v2(
            anchor=_anchor(symbol="ENAUSDT", asset="ENA"),
            source_proxies=_proxies(),
            execution_contract=build_historical_execution_contract_v2(),
            experiment_contract_sha256=EXPERIMENT_CONTRACT_SHA256,
        )
    with pytest.raises(
        HistoricalThreeFamilyConsensusContractErrorV2,
        match="proxy factory",
    ):
        HistoricalDirectionalLeafV2(
            family=HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            status=HistoricalLeafStatusV2.READY,
            direction=1,
            strength_micros=1,
            source_rule_version="x",
            source_payload_sha256="a" * 64,
            source_slice_sha256="b" * 64,
            symbol=TARGET,
            venue=VenueV2.USDM_FUTURES,
            interval="5m",
            bar_open_ms=FINAL_BAR_OPEN_MS,
            bar_close_ms=FINAL_BAR_CLOSE_MS,
            historical_slice_through_ms=FINAL_BAR_CLOSE_MS,
            source_representation=(
                HistoricalLeafSourceRepresentationV2.CANONICAL_NUMERIC_CALCULATION
            ),
            reasons=("READY",),
        )

    value = _consensus()
    original_event_id = value.event_id
    object.__setattr__(value, "event_id", "0" * 64)
    try:
        with pytest.raises(
            HistoricalThreeFamilyConsensusContractErrorV2,
            match="event ID differs",
        ):
            canonical_historical_three_family_consensus_v2(value)
    finally:
        object.__setattr__(value, "event_id", original_event_id)


def test_leaf_builder_rejects_tampered_proxy_hash() -> None:
    proxy = _price_proxy()
    original_hash = proxy.proxy_sha256
    object.__setattr__(proxy, "proxy_sha256", "not-a-canonical-sha256")
    try:
        with pytest.raises(
            HistoricalThreeFamilyConsensusContractErrorV2,
            match="price proxy_sha256 must be exactly 64 lowercase",
        ):
            build_historical_directional_leaf_v2(proxy)
    finally:
        object.__setattr__(proxy, "proxy_sha256", original_hash)
