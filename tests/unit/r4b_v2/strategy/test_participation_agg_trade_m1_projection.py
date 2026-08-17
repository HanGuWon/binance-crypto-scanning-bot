from __future__ import annotations

import copy
import hashlib
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.r4b_v2.capture import usdm_market_m1 as market_m1
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmAggTradeM1V2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.family_a_features import (
    FamilyAContractMultiplierV2,
    build_family_a_contract_multiplier_v2,
)
from signalbot.r4b_v2.strategy.participation_agg_trade_m1_projection import (
    PARTICIPATION_AGG_TRADE_M1_AUTHORITY_STATUS_V2,
    PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2,
    ParticipationAggTradeM1ProjectionContractErrorV2,
    ParticipationAggTradeM1ProjectionStatusV2,
    ParticipationAggTradeM1ProjectionV2,
    build_participation_agg_trade_m1_projection_v2,
    canonical_participation_agg_trade_m1_projection_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    ParticipationFlowStatusV2,
)

ATTEMPT_ID = "participation-m1-attempt"
SYMBOL = "BTCUSDT"
PLAN_SHA256 = "a" * 64
PROTOCOL_SHA256 = "b" * 64
CAPTURE_SHA256 = "c" * 64
MULTIPLIER_SOURCE_SHA256 = "d" * 64
MULTIPLIER_SCHEMA_SHA256 = "e" * 64
CURRENT_BAR_OPEN_MS = 2_000_160_000_000
CURRENT_BAR_CLOSE_MS = CURRENT_BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = CURRENT_BAR_CLOSE_MS + DECISION_DELAY_MS_V2
FIRST_SLOT_OPEN_MS = CURRENT_BAR_OPEN_MS - (
    (PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
)
FACTORY_TOKEN = vars(market_m1)["_FACTORY_TOKEN"]


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _row(
    index: int,
    *,
    aggregate_trade_id: int | None = None,
    first_trade_id: int | None = None,
    last_trade_id: int | None = None,
    trade_time_ms: int | None = None,
    event_ms: int | None = None,
    receipt_wall_ms: int | None = None,
    receipt_monotonic_ns: int | None = None,
    frame_seq: int | None = None,
    ingest_seq: int | None = None,
    symbol: str = SYMBOL,
    stream: str | None = None,
    promoting_plan_sha256: str = PLAN_SHA256,
    protocol_sha256: str = PROTOCOL_SHA256,
    parser_contract_sha256: str = USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    price: Decimal | None = None,
    quantity: Decimal = Decimal(10),
    normal_quantity: Decimal | None = None,
    buyer_maker: bool | None = None,
    source_tag: str | None = None,
) -> UsdmAggTradeM1V2:
    split = PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2 // 2
    second_session = index >= split
    session_offset = index - split if second_session else index
    slot_open_ms = FIRST_SLOT_OPEN_MS + index * FIVE_MINUTE_MS_V2
    actual_trade_time = slot_open_ms if trade_time_ms is None else trade_time_ms
    actual_event = actual_trade_time if event_ms is None else event_ms
    tag = str(index) if source_tag is None else source_tag
    raw_id = 50_000 + index if first_trade_id is None else first_trade_id
    return UsdmAggTradeM1V2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_market",
        stream=f"{symbol.lower()}@aggTrade" if stream is None else stream,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_authority_sha256=CAPTURE_SHA256,
        protocol_sha256=protocol_sha256,
        parser_contract_sha256=parser_contract_sha256,
        m0_leaf_sha256=_sha256(f"leaf-{tag}"),
        raw_payload_hash_v2=_sha256(f"raw-{tag}"),
        session_id="session-b" if second_session else "session-a",
        plan_id="promoting-plan-v2",
        connection_id="connection-b" if second_session else "connection-a",
        generation=1,
        frame_seq=session_offset + 1 if frame_seq is None else frame_seq,
        ingest_seq=session_offset + 1 if ingest_seq is None else ingest_seq,
        receipt_wall_ms=(actual_event + 1 if receipt_wall_ms is None else receipt_wall_ms),
        receipt_monotonic_ns=(
            (session_offset + 1) * 1_000_000
            if receipt_monotonic_ns is None
            else receipt_monotonic_ns
        ),
        event_ms=actual_event,
        aggregate_trade_id=(10_000 + index if aggregate_trade_id is None else aggregate_trade_id),
        price=(Decimal(100) + Decimal(index % 11) * Decimal("0.01") if price is None else price),
        quantity=quantity,
        normal_quantity=(Decimal(1 + index % 5) if normal_quantity is None else normal_quantity),
        first_trade_id=raw_id,
        last_trade_id=raw_id if last_trade_id is None else last_trade_id,
        trade_time_ms=actual_trade_time,
        buyer_maker=(index % 2 == 1 if buyer_maker is None else buyer_maker),
        stream_type=1,
        _factory_token=FACTORY_TOKEN,
    )


def _copy_row(
    value: UsdmAggTradeM1V2,
    *,
    aggregate_trade_id: int | None = None,
    first_trade_id: int | None = None,
    last_trade_id: int | None = None,
    trade_time_ms: int | None = None,
    event_ms: int | None = None,
    receipt_wall_ms: int | None = None,
    receipt_monotonic_ns: int | None = None,
    frame_seq: int | None = None,
    symbol: str | None = None,
    stream: str | None = None,
    promoting_plan_sha256: str | None = None,
    parser_contract_sha256: str | None = None,
    quantity: Decimal | None = None,
    normal_quantity: Decimal | None = None,
    buyer_maker: bool | None = None,
    source_tag: str | None = None,
) -> UsdmAggTradeM1V2:
    actual_symbol = value.symbol if symbol is None else symbol
    return UsdmAggTradeM1V2(
        symbol=actual_symbol,
        venue=value.venue,
        route_id=value.route_id,
        stream=(
            f"{actual_symbol.lower()}@aggTrade"
            if symbol is not None and stream is None
            else value.stream
            if stream is None
            else stream
        ),
        promoting_plan_sha256=(
            value.promoting_plan_sha256 if promoting_plan_sha256 is None else promoting_plan_sha256
        ),
        capture_authority_sha256=value.capture_authority_sha256,
        protocol_sha256=value.protocol_sha256,
        parser_contract_sha256=(
            value.parser_contract_sha256
            if parser_contract_sha256 is None
            else parser_contract_sha256
        ),
        m0_leaf_sha256=(
            value.m0_leaf_sha256 if source_tag is None else _sha256(f"leaf-{source_tag}")
        ),
        raw_payload_hash_v2=(
            value.raw_payload_hash_v2 if source_tag is None else _sha256(f"raw-{source_tag}")
        ),
        session_id=value.session_id,
        plan_id=value.plan_id,
        connection_id=value.connection_id,
        generation=value.generation,
        frame_seq=value.frame_seq if frame_seq is None else frame_seq,
        ingest_seq=value.ingest_seq,
        receipt_wall_ms=(value.receipt_wall_ms if receipt_wall_ms is None else receipt_wall_ms),
        receipt_monotonic_ns=(
            value.receipt_monotonic_ns if receipt_monotonic_ns is None else receipt_monotonic_ns
        ),
        event_ms=value.event_ms if event_ms is None else event_ms,
        aggregate_trade_id=(
            value.aggregate_trade_id if aggregate_trade_id is None else aggregate_trade_id
        ),
        price=value.price,
        quantity=value.quantity if quantity is None else quantity,
        normal_quantity=(value.normal_quantity if normal_quantity is None else normal_quantity),
        first_trade_id=(value.first_trade_id if first_trade_id is None else first_trade_id),
        last_trade_id=value.last_trade_id if last_trade_id is None else last_trade_id,
        trade_time_ms=(value.trade_time_ms if trade_time_ms is None else trade_time_ms),
        buyer_maker=value.buyer_maker if buyer_maker is None else buyer_maker,
        stream_type=value.stream_type,
        _factory_token=FACTORY_TOKEN,
    )


@cache
def _base_rows() -> tuple[UsdmAggTradeM1V2, ...]:
    return tuple(_row(index) for index in range(PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2))


@cache
def _multiplier() -> FamilyAContractMultiplierV2:
    return build_family_a_contract_multiplier_v2(
        attempt_id=ATTEMPT_ID,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        contract_multiplier=Decimal(2),
        effective_from_ms=FIRST_SLOT_OPEN_MS,
        effective_until_ms=CURRENT_BAR_CLOSE_MS,
        source_root_sha256=MULTIPLIER_SOURCE_SHA256,
        schema_sha256=MULTIPLIER_SCHEMA_SHA256,
    )


@cache
def _base_projection() -> ParticipationAggTradeM1ProjectionV2:
    return build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=tuple(reversed(_base_rows())),
        contract_multiplier_authority=_multiplier(),
    )


def test_exact_8641_slot_projection_is_numeric_but_never_producer_ready() -> None:
    projection = _base_projection()
    rows = _base_rows()

    assert projection.status is ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_READY_M1_ONLY
    assert projection.calculation is not None
    assert projection.calculation.status is ParticipationFlowStatusV2.READY
    assert projection.calculation_ready
    assert projection.calculation.direction == 1
    assert projection.authority_status == PARTICIPATION_AGG_TRADE_M1_AUTHORITY_STATUS_V2
    assert projection.data_through_ms is None
    assert projection.m2_certificate_sha256 is None
    assert not projection.causal_inputs_complete
    assert not projection.producer_ready
    assert not projection.promoting_eligible
    assert not projection.exchange_trade_capture_complete
    assert projection.slot_absence_interpretation == "UNKNOWN_NOT_ZERO"
    assert projection.all_expected_slots_nonempty_observed
    assert projection.missing_slot_open_ms == ()
    assert len(projection.observed_slot_values) == 8_641
    assert len(projection.ordered_source_lineage) == len(rows)
    assert projection.ordered_source_lineage[-1].data_time_ms == CURRENT_BAR_OPEN_MS
    assert projection.ordered_source_lineage[-1].source_event_ms == rows[-1].event_ms
    assert projection.ordered_source_lineage[-1].receipt_wall_ms == rows[-1].receipt_wall_ms
    assert projection.contract_multiplier_authority.contract_multiplier == Decimal(2)
    current = projection.observed_slot_values[-1]
    assert current.total_trade_notional == rows[-1].price * rows[-1].quantity * Decimal(2)
    assert canonical_participation_agg_trade_m1_projection_v2(projection)


def test_multiple_trades_in_one_slot_are_all_retained_and_aggregated() -> None:
    rows = _base_rows()
    extra_current_trade = _row(
        PARTICIPATION_AGG_TRADE_M1_EXPECTED_SLOT_COUNT_V2,
        trade_time_ms=CURRENT_BAR_OPEN_MS + 1,
        event_ms=CURRENT_BAR_OPEN_MS + 1,
        receipt_wall_ms=CURRENT_BAR_OPEN_MS + 2,
        source_tag="extra-current-trade",
    )
    projection = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows, extra_current_trade),
        contract_multiplier_authority=_multiplier(),
    )

    assert projection.status is ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_READY_M1_ONLY
    assert projection.calculation is not None
    assert len(projection.ordered_source_lineage) == 8_642
    assert len(projection.economic_flow_rows) == 8_642
    assert len(projection.observed_slot_values) == 8_641
    current = projection.observed_slot_values[-1]
    expected_total = (
        rows[-1].price * rows[-1].quantity
        + extra_current_trade.price * extra_current_trade.quantity
    ) * Decimal(2)
    assert current.total_trade_notional == expected_total
    assert projection.missing_slot_open_ms == ()
    assert projection.all_expected_slots_nonempty_observed


def test_buyer_maker_sign_is_side_symmetric_and_multiplier_is_sealed() -> None:
    rows = _base_rows()
    bearish_current = _copy_row(
        rows[-1],
        buyer_maker=True,
        source_tag="bearish-current",
    )
    bearish = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows[:-1], bearish_current),
        contract_multiplier_authority=_multiplier(),
    )

    assert bearish.calculation is not None
    assert bearish.calculation.status is ParticipationFlowStatusV2.READY
    assert bearish.calculation.current_signed_share is not None
    assert bearish.calculation.current_signed_share < 0
    assert bearish.calculation.direction == -1
    assert not bearish.producer_ready


def test_missing_slot_is_unknown_and_never_fabricated_as_zero_bar() -> None:
    rows = _base_rows()
    missing = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows[:100], *rows[101:]),
        contract_multiplier_authority=_multiplier(),
    )

    assert (
        missing.status is ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN
    )
    assert missing.missing_slot_open_ms == (rows[100].trade_time_ms,)
    assert len(missing.observed_slot_values) == 8_640
    assert missing.calculation is None
    assert not missing.calculation_ready
    assert not missing.producer_ready
    assert "MISSING_AGGTRADE_SLOT_IS_UNKNOWN_NOT_ZERO" in missing.reasons


def test_observed_trade_id_gap_with_full_slots_withholds_calculation() -> None:
    rows = _base_rows()
    previous = rows[-2]
    gapped_current = _copy_row(
        rows[-1],
        aggregate_trade_id=previous.aggregate_trade_id + 2,
        first_trade_id=previous.last_trade_id + 2,
        last_trade_id=previous.last_trade_id + 2,
        source_tag="gapped-current",
    )
    gapped = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows[:-1], gapped_current),
        contract_multiplier_authority=_multiplier(),
    )

    assert gapped.all_expected_slots_nonempty_observed
    assert not gapped.aggregate_id_contiguous_observed
    assert not gapped.raw_trade_id_contiguous_observed
    assert (
        gapped.status is ParticipationAggTradeM1ProjectionStatusV2.UNAVAILABLE_OBSERVED_SEQUENCE_GAP
    )
    assert gapped.calculation is None


def test_zero_normal_quantity_is_numeric_inconclusive_not_neutral_flow() -> None:
    rows = _base_rows()
    no_normal_current = _copy_row(
        rows[-1],
        normal_quantity=Decimal(0),
        source_tag="zero-normal-current",
    )
    projection = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows[:-1], no_normal_current),
        contract_multiplier_authority=_multiplier(),
    )

    assert projection.calculation is not None
    assert projection.calculation.status is ParticipationFlowStatusV2.INCONCLUSIVE_DATA
    assert projection.status is ParticipationAggTradeM1ProjectionStatusV2.NUMERIC_NONREADY_M1_ONLY
    assert projection.calculation.current_signed_share is None
    assert projection.calculation.direction == 0


def test_T_boundary_assignment_and_E_receipt_D_boundaries_are_exact() -> None:
    rows = _base_rows()
    current = _copy_row(
        rows[-1],
        trade_time_ms=CURRENT_BAR_OPEN_MS,
        event_ms=DECISION_CUTOFF_MS,
        receipt_wall_ms=DECISION_CUTOFF_MS,
        source_tag="current-at-D",
    )
    projection = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(*rows[:-1], current),
        contract_multiplier_authority=_multiplier(),
    )

    assert projection.ordered_source_lineage[-1].slot_open_ms == CURRENT_BAR_OPEN_MS
    assert projection.ordered_source_lineage[-1].data_time_ms == CURRENT_BAR_OPEN_MS
    assert projection.latest_source_event_ms == DECISION_CUTOFF_MS
    assert projection.latest_receipt_wall_ms == DECISION_CUTOFF_MS


def test_duplicate_conflict_identity_cursor_and_clock_fail_closed() -> None:
    rows = _base_rows()
    small = rows[:2]
    conflicting = _copy_row(
        small[0],
        quantity=Decimal(11),
        source_tag="conflicting-aggregate",
    )
    wrong_plan = _copy_row(
        small[1],
        promoting_plan_sha256="f" * 64,
        source_tag="wrong-plan",
    )
    duplicate_cursor = _copy_row(
        small[1],
        frame_seq=small[0].frame_seq,
        source_tag="duplicate-cursor",
    )
    bad_clock = _copy_row(
        rows[-1],
        event_ms=DECISION_CUTOFF_MS,
        receipt_wall_ms=DECISION_CUTOFF_MS - 1,
        source_tag="bad-clock",
    )

    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="duplicate"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(*small, small[0]),
            contract_multiplier_authority=_multiplier(),
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="conflicting"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(small[0], conflicting),
            contract_multiplier_authority=_multiplier(),
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="disagree"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(small[0], wrong_plan),
            contract_multiplier_authority=_multiplier(),
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="cursor"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(small[0], duplicate_cursor),
            contract_multiplier_authority=_multiplier(),
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="noncausal"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(bad_clock,),
            contract_multiplier_authority=_multiplier(),
        )


def test_outside_T_and_post_mint_parser_tamper_fail_closed() -> None:
    rows = _base_rows()
    outside = _copy_row(
        rows[-1],
        trade_time_ms=CURRENT_BAR_CLOSE_MS + 1,
        event_ms=CURRENT_BAR_CLOSE_MS + 1,
        receipt_wall_ms=CURRENT_BAR_CLOSE_MS + 2,
        source_tag="outside-current",
    )
    parser_tamper = copy.copy(rows[0])
    object.__setattr__(parser_tamper, "parser_contract_sha256", "f" * 64)

    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="outside"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(outside,),
            contract_multiplier_authority=_multiplier(),
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="canonical"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=(parser_tamper,),
            contract_multiplier_authority=_multiplier(),
        )


def test_multiplier_authority_is_required_covers_window_and_rejects_tamper() -> None:
    rows = _base_rows()[:1]
    short = build_family_a_contract_multiplier_v2(
        attempt_id=ATTEMPT_ID,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        contract_multiplier=Decimal(2),
        effective_from_ms=FIRST_SLOT_OPEN_MS + 1,
        effective_until_ms=CURRENT_BAR_CLOSE_MS,
        source_root_sha256=MULTIPLIER_SOURCE_SHA256,
        schema_sha256=MULTIPLIER_SCHEMA_SHA256,
    )
    tampered = copy.copy(_multiplier())
    object.__setattr__(tampered, "contract_multiplier", Decimal(1))

    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="requires sealed"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=rows,
            contract_multiplier_authority=None,  # type: ignore[arg-type]
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="cover"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=rows,
            contract_multiplier_authority=short,
        )
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="sealed version"):
        build_participation_agg_trade_m1_projection_v2(
            attempt_id=ATTEMPT_ID,
            bar_open_ms=CURRENT_BAR_OPEN_MS,
            rows=rows,
            contract_multiplier_authority=tampered,
        )


def test_source_receipt_change_does_not_change_economic_flow_root() -> None:
    rows = _base_rows()[:2]
    baseline = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=rows,
        contract_multiplier_authority=_multiplier(),
    )
    changed_row = _copy_row(
        rows[0],
        receipt_wall_ms=rows[0].receipt_wall_ms + 1,
        receipt_monotonic_ns=rows[0].receipt_monotonic_ns + 1,
        source_tag="changed-receipt",
    )
    changed = build_participation_agg_trade_m1_projection_v2(
        attempt_id=ATTEMPT_ID,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=(changed_row, rows[1]),
        contract_multiplier_authority=_multiplier(),
    )

    assert changed.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert changed.economic_flow_root_sha256 == baseline.economic_flow_root_sha256


def test_factory_and_post_mint_projection_tamper_fail_closed() -> None:
    projection = _base_projection()
    constructor_values = {
        item.name: getattr(projection, item.name) for item in fields(projection) if item.init
    }
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="factory"):
        ParticipationAggTradeM1ProjectionV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="factory"):
        replace(projection, symbol="ETHUSDT")

    tampered = copy.copy(projection)
    object.__setattr__(tampered, "economic_flow_root_sha256", "0" * 64)
    with pytest.raises(ParticipationAggTradeM1ProjectionContractErrorV2, match="sealed root"):
        canonical_participation_agg_trade_m1_projection_v2(tampered)
