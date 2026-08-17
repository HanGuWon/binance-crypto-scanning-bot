from __future__ import annotations

import json
from copy import copy
from dataclasses import fields
from decimal import Decimal
from functools import cache
from inspect import signature

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import RobustZStatusV2
from signalbot.r4b_v2.strategy.price_evidence import calculate_price_close_path_v2
from signalbot.r4b_v2.strategy.price_historical_kline_proxy import (
    PRICE_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2,
    PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2,
    PriceHistoricalKlineCloseEntryV2,
    PriceHistoricalKlineProxyContractErrorV2,
    PriceHistoricalKlineProxyStatusV2,
    PriceHistoricalKlineProxyV2,
    PriceHistoricalKlineSourceEntryV2,
    build_price_historical_kline_proxy_v2,
    canonical_price_historical_kline_proxy_v2,
)

ATTEMPT_ID = "price-historical-kline-proxy-test"
DATASET_SHA256 = "a" * 64
SYMBOL = "BTCUSDT"
CURRENT_BAR_OPEN_MS = 2_000_160_000_000
FIRST_SLOT_OPEN_MS = CURRENT_BAR_OPEN_MS - (
    (PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
)


def _candle(
    index: int,
    *,
    open_time_ms: int | None = None,
    market: Market = Market.FUTURES,
    symbol: str = SYMBOL,
    interval: str = "5m",
    close: Decimal | None = None,
    is_closed: bool = True,
) -> Candle:
    actual_open_ms = (
        FIRST_SLOT_OPEN_MS + index * FIVE_MINUTE_MS_V2
        if open_time_ms is None
        else open_time_ms
    )
    actual_close = (
        Decimal(100)
        + Decimal(index) * Decimal("0.001")
        + Decimal(index % 7) * Decimal("0.0001")
        if close is None
        else close
    )
    return Candle(
        market=market,
        symbol=symbol,
        interval=interval,
        open_time_ms=actual_open_ms,
        close_time_ms=actual_open_ms + FIVE_MINUTE_MS_V2 - 1,
        open=actual_close,
        high=actual_close + Decimal(1),
        low=actual_close - Decimal(1),
        close=actual_close,
        volume=Decimal(10),
        quote_volume=Decimal(1_000 + index % 17),
        trade_count=100 + index,
        taker_buy_base_volume=Decimal(4),
        taker_buy_quote_volume=Decimal(400),
        is_closed=is_closed,
    )


@cache
def _base_rows() -> tuple[Candle, ...]:
    return tuple(
        _candle(index)
        for index in range(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)
    )


@cache
def _base_proxy() -> PriceHistoricalKlineProxyV2:
    return _build(_base_rows())


def _build(rows: tuple[Candle, ...]) -> PriceHistoricalKlineProxyV2:
    return build_price_historical_kline_proxy_v2(
        attempt_id=ATTEMPT_ID,
        dataset_sha256=DATASET_SHA256,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=rows,
    )


def _replace_row(
    rows: tuple[Candle, ...],
    index: int,
    replacement: Candle,
) -> tuple[Candle, ...]:
    actual_index = index % len(rows)
    return (*rows[:actual_index], replacement, *rows[actual_index + 1 :])


def test_exact_8653_closed_rows_reuse_frozen_calculation_without_authority() -> None:
    proxy = _base_proxy()
    expected = calculate_price_close_path_v2(tuple(row.close for row in _base_rows()))

    assert proxy.status is PriceHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY
    assert proxy.calculation.status is RobustZStatusV2.READY
    assert proxy.calculation == expected
    assert proxy.calculation_ready
    assert proxy.direction == proxy.calculation.direction == 1
    assert proxy.strength_micros == proxy.calculation.strength_micros > 0
    assert len(proxy.ordered_source_rows) == 8_653
    assert len(proxy.economic_close_slice) == 8_653
    assert proxy.ordered_source_rows[-1].open_time_ms == CURRENT_BAR_OPEN_MS
    assert proxy.authority_status == PRICE_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2
    assert proxy.historical_only is True
    assert proxy.historical_diagnostic_only is True
    assert proxy.outcome_data_read is False
    assert proxy.target_return_used is False
    assert proxy.exact_kline_m1_equivalent is False
    assert proxy.verified_raw_membership_m0_bound is False
    assert proxy.strict_source_parser_m1_bound is False
    assert proxy.causal_cursor_finality_m2_bound is False
    assert proxy.causal_inputs_complete is False
    assert proxy.producer_ready is False
    assert proxy.promoting_eligible is False
    assert proxy.probability_eligible is False
    assert proxy.probability_calibrated is False
    assert proxy.target_return_eligible is False
    assert proxy.data_through_ms is None
    assert proxy.m0_root_sha256 is None
    assert proxy.m1_payload_sha256 is None
    assert proxy.m2_certificate_sha256 is None
    assert len(proxy.source_lineage_root_sha256) == 64
    assert len(proxy.economic_close_slice_sha256) == 64

    document = json.loads(canonical_price_historical_kline_proxy_v2(proxy))
    assert document["target_return_used"] is False
    assert document["probability_eligible"] is False
    assert "target_return" not in document


def test_exact_constant_close_path_is_numeric_nonready_and_neutral() -> None:
    constant_rows = tuple(
        _candle(index, close=Decimal(100))
        for index in range(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)
    )
    proxy = _build(constant_rows)

    assert proxy.status is PriceHistoricalKlineProxyStatusV2.NUMERIC_NONREADY_PROXY
    assert proxy.calculation.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert not proxy.calculation_ready
    assert proxy.direction == 0
    assert proxy.strength_micros == 0
    assert proxy.calculation.composite is None


def test_builder_surface_cannot_accept_labels_fills_outcomes_or_returns() -> None:
    assert tuple(signature(build_price_historical_kline_proxy_v2).parameters) == (
        "attempt_id",
        "dataset_sha256",
        "bar_open_ms",
        "rows",
    )
    assert "M0_M1_M2_SOURCE_AUTHORITY_UNBOUND" in _base_proxy().reasons


def test_missing_and_extra_rows_fail_before_numeric_calculation() -> None:
    rows = _base_rows()
    future = _candle(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)

    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="exactly 8,653"):
        _build(rows[1:])
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="exactly 8,653"):
        _build((*rows, future))


def test_gap_duplicate_and_input_order_fail_closed() -> None:
    rows = _base_rows()
    earlier_first = rows[0].model_copy(
        update={
            "open_time_ms": rows[0].open_time_ms - FIVE_MINUTE_MS_V2,
            "close_time_ms": rows[0].close_time_ms - FIVE_MINUTE_MS_V2,
        }
    )

    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="gap"):
        _build(_replace_row(rows, 0, earlier_first))
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="duplicate"):
        _build(_replace_row(rows, 100, rows[99]))
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="order"):
        _build((*rows[:100], rows[101], rows[100], *rows[102:]))


def test_unclosed_and_future_rows_fail_closed() -> None:
    rows = _base_rows()
    unclosed = rows[-1].model_copy(update={"is_closed": False})
    future = _candle(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)

    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="fully closed"):
        _build(_replace_row(rows, -1, unclosed))
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="future row"):
        _build(_replace_row(rows, -1, future))


@pytest.mark.parametrize(
    ("replacement", "match"),
    (
        (lambda row: row.model_copy(update={"market": Market.SPOT}), "futures"),
        (lambda row: row.model_copy(update={"interval": "1h"}), "closed 5m"),
        (lambda row: row.model_copy(update={"symbol": "ETHUSDT"}), "share one"),
    ),
)
def test_wrong_market_interval_or_symbol_identity_fails_closed(
    replacement: object,
    match: str,
) -> None:
    rows = _base_rows()
    assert callable(replacement)
    changed = replacement(rows[100])
    assert isinstance(changed, Candle)

    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match=match):
        _build(_replace_row(rows, 100, changed))


@pytest.mark.parametrize("close", (Decimal(0), Decimal("NaN"), Decimal("Infinity")))
def test_nonpositive_or_nonfinite_close_fails_closed(close: Decimal) -> None:
    rows = _base_rows()
    invalid = rows[-1].model_copy(update={"close": close})
    match = "finite Decimal" if not close.is_finite() else "positive"

    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match=match):
        _build(_replace_row(rows, -1, invalid))


def test_future_row_mutation_cannot_change_decision_slice_or_hashes() -> None:
    proxy = _base_proxy()
    before = canonical_price_historical_kline_proxy_v2(proxy)
    future = _candle(PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2)

    object.__setattr__(future, "close", future.close + Decimal(50))

    assert canonical_price_historical_kline_proxy_v2(proxy) == before
    assert proxy.ordered_source_rows[-1].open_time_ms == CURRENT_BAR_OPEN_MS
    assert all(row.bar_open_ms <= CURRENT_BAR_OPEN_MS for row in proxy.economic_close_slice)


def test_source_only_change_changes_source_hash_but_not_close_slice_or_score() -> None:
    baseline = _base_proxy()
    rows = _base_rows()
    source_only = rows[500].model_copy(update={"high": rows[500].high + Decimal(5)})
    changed = _build(_replace_row(rows, 500, source_only))

    assert changed.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert changed.economic_close_slice_sha256 == baseline.economic_close_slice_sha256
    assert changed.calculation == baseline.calculation
    assert changed.direction == baseline.direction
    assert changed.strength_micros == baseline.strength_micros


def test_factories_and_canonical_live_validation_reject_tampering() -> None:
    proxy = _base_proxy()
    proxy_values = {
        item.name: getattr(proxy, item.name)
        for item in fields(PriceHistoricalKlineProxyV2)
        if item.init
    }
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="factory"):
        PriceHistoricalKlineProxyV2(**proxy_values)  # type: ignore[arg-type]

    source = proxy.ordered_source_rows[0]
    source_values = {
        item.name: getattr(source, item.name)
        for item in fields(PriceHistoricalKlineSourceEntryV2)
        if item.init
    }
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="factory"):
        PriceHistoricalKlineSourceEntryV2(**source_values)  # type: ignore[arg-type]

    close = proxy.economic_close_slice[0]
    close_values = {
        item.name: getattr(close, item.name)
        for item in fields(PriceHistoricalKlineCloseEntryV2)
        if item.init
    }
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="factory"):
        PriceHistoricalKlineCloseEntryV2(**close_values)  # type: ignore[arg-type]

    authority_tamper = copy(proxy)
    object.__setattr__(authority_tamper, "producer_ready", True)
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="claims"):
        canonical_price_historical_kline_proxy_v2(authority_tamper)

    root_tamper = copy(proxy)
    object.__setattr__(root_tamper, "source_lineage_root_sha256", "b" * 64)
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="sealed root"):
        canonical_price_historical_kline_proxy_v2(root_tamper)

    direction_tamper = copy(proxy)
    object.__setattr__(direction_tamper, "direction", -proxy.direction)
    with pytest.raises(PriceHistoricalKlineProxyContractErrorV2, match="numeric summary"):
        canonical_price_historical_kline_proxy_v2(direction_tamper)
