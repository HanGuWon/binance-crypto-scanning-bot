from __future__ import annotations

import copy
import hashlib
from collections.abc import Callable
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.r4b_v2.capture import usdm_market_m1 as market_m1
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmKline5mM1V2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import RobustZStatusV2
from signalbot.r4b_v2.strategy.price_evidence import calculate_price_close_path_v2
from signalbot.r4b_v2.strategy.price_kline_m1_projection import (
    PRICE_KLINE_M1_AUTHORITY_STATUS_V2,
    PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2,
    PriceKlineM1ProjectionContractErrorV2,
    PriceKlineM1ProjectionV2,
    build_price_kline_m1_projection_v2,
    canonical_price_kline_m1_projection_v2,
)

CURRENT_BAR_OPEN_MS = 2_000_160_000_000
CURRENT_BAR_CLOSE_MS = CURRENT_BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = CURRENT_BAR_CLOSE_MS + DECISION_DELAY_MS_V2
FIRST_BAR_OPEN_MS = CURRENT_BAR_OPEN_MS - (
    (PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
)
PLAN_SHA256 = "a" * 64
PROTOCOL_SHA256 = "b" * 64
CAPTURE_SHA256 = "c" * 64
FACTORY_TOKEN = vars(market_m1)["_FACTORY_TOKEN"]


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _row(
    index: int,
    *,
    close: Decimal | None = None,
    symbol: str = "BTCUSDT",
    stream: str | None = None,
    promoting_plan_sha256: str = PLAN_SHA256,
    protocol_sha256: str = PROTOCOL_SHA256,
    parser_contract_sha256: str = USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    plan_id: str = "promoting-plan-v2",
    session_id: str | None = None,
    connection_id: str | None = None,
    generation: int = 1,
    frame_seq: int | None = None,
    ingest_seq: int | None = None,
    bar_open_ms: int | None = None,
    event_ms: int | None = None,
    receipt_wall_ms: int | None = None,
    receipt_monotonic_ns: int | None = None,
    closed: bool = True,
    high: Decimal | None = None,
    low: Decimal | None = None,
    source_tag: str | None = None,
) -> UsdmKline5mM1V2:
    split = PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2 // 2
    in_second_session = index >= split
    session_offset = index - split if in_second_session else index
    actual_session = (
        session_id
        if session_id is not None
        else ("session-b" if in_second_session else "session-a")
    )
    actual_connection = (
        connection_id
        if connection_id is not None
        else ("connection-b" if in_second_session else "connection-a")
    )
    actual_open_ms = (
        FIRST_BAR_OPEN_MS + index * FIVE_MINUTE_MS_V2 if bar_open_ms is None else bar_open_ms
    )
    actual_close_ms = actual_open_ms + FIVE_MINUTE_MS_V2 - 1
    actual_close = (
        Decimal(100) + Decimal(index) * Decimal("0.001") + Decimal(index % 7) * Decimal("0.0001")
        if close is None
        else close
    )
    tag = str(index) if source_tag is None else source_tag
    return UsdmKline5mM1V2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_market",
        stream=f"{symbol.lower()}@kline_5m" if stream is None else stream,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_authority_sha256=CAPTURE_SHA256,
        protocol_sha256=protocol_sha256,
        parser_contract_sha256=parser_contract_sha256,
        m0_leaf_sha256=_sha256(f"leaf-{tag}"),
        raw_payload_hash_v2=_sha256(f"raw-{tag}"),
        session_id=actual_session,
        plan_id=plan_id,
        connection_id=actual_connection,
        generation=generation,
        frame_seq=(session_offset + 1 if frame_seq is None else frame_seq),
        ingest_seq=(session_offset + 1 if ingest_seq is None else ingest_seq),
        receipt_wall_ms=(actual_close_ms + 1 if receipt_wall_ms is None else receipt_wall_ms),
        receipt_monotonic_ns=(
            (session_offset + 1) * 1_000_000
            if receipt_monotonic_ns is None
            else receipt_monotonic_ns
        ),
        event_ms=actual_close_ms if event_ms is None else event_ms,
        interval="5m",
        bar_open_ms=actual_open_ms,
        bar_close_ms=actual_close_ms,
        first_trade_id=index * 2,
        last_trade_id=index * 2 + 1,
        open=actual_close - Decimal("0.1"),
        close=actual_close,
        high=actual_close + Decimal(1) if high is None else high,
        low=actual_close - Decimal(1) if low is None else low,
        base_volume=Decimal(10),
        trade_count=10,
        closed=closed,
        quote_volume=Decimal(1_000),
        taker_buy_base_volume=Decimal(4),
        taker_buy_quote_volume=Decimal(400),
        ignored_volume=Decimal(0),
        _factory_token=FACTORY_TOKEN,
    )


def _copy_row(
    value: UsdmKline5mM1V2,
    *,
    symbol: str | None = None,
    stream: str | None = None,
    promoting_plan_sha256: str | None = None,
    protocol_sha256: str | None = None,
    parser_contract_sha256: str | None = None,
    frame_seq: int | None = None,
    ingest_seq: int | None = None,
    event_ms: int | None = None,
    receipt_wall_ms: int | None = None,
    receipt_monotonic_ns: int | None = None,
    closed: bool | None = None,
    close: Decimal | None = None,
    high: Decimal | None = None,
    low: Decimal | None = None,
    source_tag: str | None = None,
) -> UsdmKline5mM1V2:
    actual_symbol = value.symbol if symbol is None else symbol
    return UsdmKline5mM1V2(
        symbol=actual_symbol,
        venue=value.venue,
        route_id=value.route_id,
        stream=(
            f"{actual_symbol.lower()}@kline_5m"
            if symbol is not None and stream is None
            else value.stream
            if stream is None
            else stream
        ),
        promoting_plan_sha256=(
            value.promoting_plan_sha256 if promoting_plan_sha256 is None else promoting_plan_sha256
        ),
        capture_authority_sha256=value.capture_authority_sha256,
        protocol_sha256=(value.protocol_sha256 if protocol_sha256 is None else protocol_sha256),
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
        ingest_seq=value.ingest_seq if ingest_seq is None else ingest_seq,
        receipt_wall_ms=(value.receipt_wall_ms if receipt_wall_ms is None else receipt_wall_ms),
        receipt_monotonic_ns=(
            value.receipt_monotonic_ns if receipt_monotonic_ns is None else receipt_monotonic_ns
        ),
        event_ms=value.event_ms if event_ms is None else event_ms,
        interval=value.interval,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        first_trade_id=value.first_trade_id,
        last_trade_id=value.last_trade_id,
        open=value.open,
        close=value.close if close is None else close,
        high=value.high if high is None else high,
        low=value.low if low is None else low,
        base_volume=value.base_volume,
        trade_count=value.trade_count,
        closed=value.closed if closed is None else closed,
        quote_volume=value.quote_volume,
        taker_buy_base_volume=value.taker_buy_base_volume,
        taker_buy_quote_volume=value.taker_buy_quote_volume,
        ignored_volume=value.ignored_volume,
        _factory_token=FACTORY_TOKEN,
    )


@cache
def _base_rows() -> tuple[UsdmKline5mM1V2, ...]:
    return tuple(_row(index) for index in range(PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2))


@cache
def _base_projection() -> PriceKlineM1ProjectionV2:
    return build_price_kline_m1_projection_v2(tuple(reversed(_base_rows())))


def test_exact_m1_projection_reuses_frozen_calculation_without_authority_upgrade() -> None:
    projection = _base_projection()
    rows = _base_rows()

    assert projection.calculation.status is RobustZStatusV2.READY
    assert projection.calculation_ready
    assert projection.authority_status == PRICE_KLINE_M1_AUTHORITY_STATUS_V2
    assert projection.data_through_ms is None
    assert projection.m2_certificate_sha256 is None
    assert not projection.causal_inputs_complete
    assert not projection.producer_ready
    assert not projection.promoting_eligible
    assert projection.assumed_closed_bar_through_ms == CURRENT_BAR_CLOSE_MS
    assert projection.decision_cutoff_ms == DECISION_CUTOFF_MS
    assert projection.source_row_count == 8_654
    assert projection.calculation_row_count == 8_653
    assert len(projection.ordered_source_lineage) == 8_654
    assert len(projection.economic_close_slice) == 8_653
    assert projection.ordered_source_lineage[0].bar_open_ms == FIRST_BAR_OPEN_MS
    assert projection.economic_close_slice[0].close == rows[1].close
    assert projection.ordered_source_lineage[-1].source_event_ms == rows[-1].event_ms
    assert projection.ordered_source_lineage[-1].data_time_ms == rows[-1].bar_close_ms
    assert projection.ordered_source_lineage[-1].receipt_wall_ms == rows[-1].receipt_wall_ms
    assert (
        projection.ordered_source_lineage[0].session_id
        != projection.ordered_source_lineage[-1].session_id
    )
    split = PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2 // 2
    assert (
        projection.ordered_source_lineage[split].frame_seq
        < projection.ordered_source_lineage[split - 1].frame_seq
    )
    assert projection.calculation == calculate_price_close_path_v2(
        tuple(row.close for row in rows[1:])
    )
    assert not hasattr(projection.ordered_source_lineage[0], "open_event_id")
    assert canonical_price_kline_m1_projection_v2(projection)


def test_high_low_changes_only_source_lineage_not_close_economics_or_calculation() -> None:
    baseline = _base_projection()
    rows = _base_rows()
    changed_row = _copy_row(
        rows[500],
        high=rows[500].close + Decimal(10),
        low=rows[500].close - Decimal(10),
        source_tag="changed-range-500",
    )
    changed = build_price_kline_m1_projection_v2((*rows[:500], changed_row, *rows[501:]))

    assert changed.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert changed.economic_close_slice_sha256 == baseline.economic_close_slice_sha256
    assert changed.calculation == baseline.calculation
    assert changed.projection_sha256 != baseline.projection_sha256


def test_missing_anchor_gap_duplicate_and_conflicting_slot_fail_closed() -> None:
    rows = _base_rows()
    duplicate = (*rows[:101], rows[100], *rows[102:])
    conflicting = (
        *rows[:101],
        _copy_row(
            rows[100],
            close=rows[100].close + Decimal("0.01"),
            high=rows[100].high + Decimal("0.01"),
            source_tag="conflicting-slot-100",
        ),
        *rows[102:],
    )
    future = _row(
        PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2,
        source_tag="future-after-gap",
    )
    gap = (*rows[:100], *rows[101:], future)

    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="8,654"):
        build_price_kline_m1_projection_v2(rows[1:])
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="duplicate"):
        build_price_kline_m1_projection_v2(duplicate)
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="conflicting"):
        build_price_kline_m1_projection_v2(conflicting)
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="contiguous"):
        build_price_kline_m1_projection_v2(gap)


@pytest.mark.parametrize(
    ("replacement", "match"),
    (
        (lambda row: _copy_row(row, closed=False), "x=true"),
        (lambda row: _copy_row(row, symbol="ETHUSDT"), "disagree"),
        (
            lambda row: _copy_row(row, promoting_plan_sha256="d" * 64),
            "disagree",
        ),
        (
            lambda row: _copy_row(row, protocol_sha256="e" * 64),
            "disagree",
        ),
    ),
)
def test_unclosed_or_wrong_source_identity_fails_closed(
    replacement: Callable[[UsdmKline5mM1V2], UsdmKline5mM1V2],
    match: str,
) -> None:
    rows = _base_rows()
    changed = replacement(rows[100])

    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match=match):
        build_price_kline_m1_projection_v2((*rows[:100], changed, *rows[101:]))


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    (
        ("stream", "btcusdt@aggTrade"),
        ("parser_contract_sha256", "f" * 64),
    ),
)
def test_post_mint_wrong_stream_or_parser_fails_live_validation(
    field_name: str,
    field_value: str,
) -> None:
    rows = _base_rows()
    changed = copy.copy(rows[100])
    object.__setattr__(changed, field_name, field_value)

    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="live-valid"):
        build_price_kline_m1_projection_v2((*rows[:100], changed, *rows[101:]))


@pytest.mark.parametrize(
    ("changed", "match"),
    (
        (
            _copy_row(
                _base_rows()[-1],
                event_ms=CURRENT_BAR_CLOSE_MS - 1,
            ),
            "noncausal",
        ),
        (
            _copy_row(
                _base_rows()[-1],
                event_ms=CURRENT_BAR_CLOSE_MS + 1,
                receipt_wall_ms=CURRENT_BAR_CLOSE_MS,
            ),
            "noncausal",
        ),
        (
            _copy_row(
                _base_rows()[-1],
                receipt_wall_ms=DECISION_CUTOFF_MS + 1,
            ),
            "noncausal",
        ),
        (
            _copy_row(
                _base_rows()[100],
                frame_seq=_base_rows()[99].frame_seq,
                source_tag="duplicate-cursor-100",
            ),
            "cursor",
        ),
        (
            _copy_row(
                _base_rows()[100],
                receipt_wall_ms=_base_rows()[101].receipt_wall_ms + 1,
                source_tag="wall-order-100",
            ),
            "receipt",
        ),
        (
            _copy_row(
                _base_rows()[-1],
                receipt_monotonic_ns=_base_rows()[-2].receipt_monotonic_ns,
                source_tag="mono-order-current",
            ),
            "receipt",
        ),
    ),
)
def test_bad_event_receipt_or_cursor_order_fails_closed(
    changed: UsdmKline5mM1V2,
    match: str,
) -> None:
    rows = _base_rows()
    index = next(index for index, row in enumerate(rows) if row.bar_open_ms == changed.bar_open_ms)
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match=match):
        build_price_kline_m1_projection_v2((*rows[:index], changed, *rows[index + 1 :]))


def test_zero_scale_is_numeric_nonready_and_still_never_promoting() -> None:
    rows = tuple(
        _row(index, close=Decimal(100), source_tag=f"constant-{index}")
        for index in range(PRICE_KLINE_M1_SOURCE_ROW_COUNT_V2)
    )
    projection = build_price_kline_m1_projection_v2(rows)

    assert projection.calculation.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert not projection.calculation_ready
    assert projection.calculation.composite is None
    assert not projection.producer_ready
    assert not projection.promoting_eligible


def test_upstream_rfc8785_integer_boundary_is_inherited_exactly() -> None:
    rows = _base_rows()
    maximum = 2**53 - 1
    boundary_row = _copy_row(
        rows[-1],
        receipt_monotonic_ns=maximum,
        source_tag="canonical-integer-boundary",
    )
    boundary = build_price_kline_m1_projection_v2((*rows[:-1], boundary_row))

    assert boundary.latest_receipt_monotonic_ns == maximum
    above_boundary = copy.copy(boundary_row)
    object.__setattr__(above_boundary, "receipt_monotonic_ns", maximum + 1)
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="live-valid"):
        build_price_kline_m1_projection_v2((*rows[:-1], above_boundary))


def test_factory_guards_and_post_mint_tampering_fail_closed() -> None:
    projection = _base_projection()
    constructor_values = {
        item.name: getattr(projection, item.name) for item in fields(projection) if item.init
    }

    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="factory"):
        PriceKlineM1ProjectionV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="factory"):
        replace(projection, symbol="ETHUSDT")

    tampered_projection = copy.copy(projection)
    object.__setattr__(tampered_projection, "source_lineage_root_sha256", "0" * 64)
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="sealed root"):
        canonical_price_kline_m1_projection_v2(tampered_projection)

    rows = _base_rows()
    tampered_row = _copy_row(rows[100], source_tag="post-mint-tamper")
    object.__setattr__(tampered_row, "close", tampered_row.close + Decimal(1))
    with pytest.raises(PriceKlineM1ProjectionContractErrorV2, match="live-valid"):
        build_price_kline_m1_projection_v2((*rows[:100], tampered_row, *rows[101:]))
