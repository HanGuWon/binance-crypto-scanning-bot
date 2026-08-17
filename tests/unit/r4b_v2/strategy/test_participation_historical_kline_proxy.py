from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
from functools import cache
from inspect import signature

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.strategy.participation_evidence import ParticipationFlowStatusV2
from signalbot.r4b_v2.strategy.participation_historical_kline_proxy import (
    PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2,
    PARTICIPATION_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2,
    PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2,
    ParticipationHistoricalKlineEconomicEntryV2,
    ParticipationHistoricalKlineProxyContractErrorV2,
    ParticipationHistoricalKlineProxyStatusV2,
    ParticipationHistoricalKlineProxyV2,
    build_participation_historical_kline_proxy_v2,
    canonical_participation_historical_kline_proxy_v2,
)

ATTEMPT_ID = "historical-kline-proxy-test"
DATASET_SHA256 = "a" * 64
SYMBOL = "BTCUSDT"
CURRENT_BAR_OPEN_MS = 2_000_160_000_000
FIRST_SLOT_OPEN_MS = CURRENT_BAR_OPEN_MS - (
    (PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
)


def _candle(
    index: int,
    *,
    open_time_ms: int | None = None,
    market: Market = Market.FUTURES,
    symbol: str = SYMBOL,
    interval: str = "5m",
    quote_volume: Decimal | None = None,
    taker_buy_quote_volume: Decimal | None = None,
    close: Decimal = Decimal(100),
    high: Decimal = Decimal(101),
    volume: Decimal = Decimal(10),
    taker_buy_base_volume: Decimal = Decimal(4),
    is_closed: bool = True,
) -> Candle:
    actual_open = (
        FIRST_SLOT_OPEN_MS + index * FIVE_MINUTE_MS_V2 if open_time_ms is None else open_time_ms
    )
    quote = Decimal(1_000 + index % 17) if quote_volume is None else quote_volume
    buy_fraction = Decimal("0.40") + Decimal(index % 7) * Decimal("0.01")
    taker_buy = quote * buy_fraction if taker_buy_quote_volume is None else taker_buy_quote_volume
    return Candle(
        market=market,
        symbol=symbol,
        interval=interval,
        open_time_ms=actual_open,
        close_time_ms=actual_open + FIVE_MINUTE_MS_V2 - 1,
        open=Decimal(100),
        high=high,
        low=Decimal(99),
        close=close,
        volume=volume,
        quote_volume=quote,
        trade_count=100 + index,
        taker_buy_base_volume=taker_buy_base_volume,
        taker_buy_quote_volume=taker_buy,
        is_closed=is_closed,
    )


@cache
def _base_rows() -> tuple[Candle, ...]:
    return tuple(
        _candle(index)
        for index in range(PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2)
    )


@cache
def _base_projection() -> ParticipationHistoricalKlineProxyV2:
    return build_participation_historical_kline_proxy_v2(
        attempt_id=ATTEMPT_ID,
        dataset_sha256=DATASET_SHA256,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=tuple(reversed(_base_rows())),
    )


def _small_projection(
    rows: tuple[Candle, ...],
    *,
    dataset_sha256: str = DATASET_SHA256,
) -> ParticipationHistoricalKlineProxyV2:
    return build_participation_historical_kline_proxy_v2(
        attempt_id=ATTEMPT_ID,
        dataset_sha256=dataset_sha256,
        bar_open_ms=CURRENT_BAR_OPEN_MS,
        rows=rows,
    )


def test_exact_8641_closed_klines_reuse_shared_formula_without_live_authority() -> None:
    projection = _base_projection()
    current_source = _base_rows()[-1]
    current = projection.observed_slot_values[-1]

    assert projection.status is ParticipationHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY
    assert projection.calculation is not None
    assert projection.calculation.status is ParticipationFlowStatusV2.READY
    assert projection.calculation_ready
    assert len(projection.ordered_source_rows) == 8_641
    assert len(projection.economic_flow_rows) == 8_641
    assert len(projection.observed_slot_values) == 8_641
    assert current.total_trade_notional == current_source.quote_volume
    assert current.normal_notional == current_source.quote_volume
    assert current.signed_normal_notional == (
        Decimal(2) * current_source.taker_buy_quote_volume - current_source.quote_volume
    )
    assert current.signed_share == current.signed_normal_notional / current_source.quote_volume
    assert projection.proxy_assumption == PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2
    assert projection.authority_status == PARTICIPATION_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2
    assert projection.outcome_data_read is False
    assert projection.exact_agg_trade_m1_equivalent is False
    assert projection.verified_raw_membership_m0_bound is False
    assert projection.strict_source_parser_m1_bound is False
    assert projection.causal_cursor_finality_m2_bound is False
    assert projection.causal_inputs_complete is False
    assert projection.producer_ready is False
    assert projection.promoting_eligible is False
    assert projection.probability_eligible is False
    assert projection.data_through_ms is None
    assert projection.m0_root_sha256 is None
    assert projection.m1_payload_sha256 is None
    assert projection.m2_certificate_sha256 is None
    assert canonical_participation_historical_kline_proxy_v2(projection)


def test_builder_surface_has_no_outcome_label_fill_or_return_input() -> None:
    assert tuple(signature(build_participation_historical_kline_proxy_v2).parameters) == (
        "attempt_id",
        "dataset_sha256",
        "bar_open_ms",
        "rows",
    )
    assert "OUTCOME_LABEL_FILL_AND_RETURN_DATA_NOT_READ" in _base_projection().reasons


def test_present_zero_quote_bar_is_inconclusive_not_a_neutral_flow_fallback() -> None:
    rows = _base_rows()
    zero_current = rows[-1].model_copy(
        update={
            "quote_volume": Decimal(0),
            "taker_buy_quote_volume": Decimal(0),
        }
    )
    projection = _small_projection((*rows[:-1], zero_current))

    assert projection.status is ParticipationHistoricalKlineProxyStatusV2.NUMERIC_NONREADY_PROXY
    assert projection.calculation is not None
    assert projection.calculation.status is ParticipationFlowStatusV2.INCONCLUSIVE_DATA
    assert projection.observed_slot_values[-1].signed_share is None
    assert projection.missing_slot_open_ms == ()
    assert not projection.calculation_ready


def test_missing_edge_and_internal_gap_are_unknown_never_zero_filled() -> None:
    current_only = _small_projection((_base_rows()[-1],))
    assert (
        current_only.status
        is ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN
    )
    assert len(current_only.missing_slot_open_ms) == 8_640
    assert current_only.internal_gap_after_open_ms == ()
    assert current_only.calculation is None
    assert current_only.slot_absence_interpretation == "UNKNOWN_NOT_ZERO"
    assert len(current_only.observed_slot_values) == 1

    rows = _base_rows()
    missing_index = 4_000
    gapped = _small_projection((*rows[:missing_index], *rows[missing_index + 1 :]))
    assert (
        gapped.status is ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_INTERNAL_GAP_UNKNOWN
    )
    assert gapped.missing_slot_open_ms == (rows[missing_index].open_time_ms,)
    assert gapped.internal_gap_after_open_ms == (rows[missing_index - 1].open_time_ms,)
    assert gapped.calculation is None
    assert len(gapped.observed_slot_values) == 8_640


def test_signed_share_decimal_boundaries_are_exact_and_zero_total_is_unknown() -> None:
    rows = (
        _candle(8_637, quote_volume=Decimal(100), taker_buy_quote_volume=Decimal(0)),
        _candle(8_638, quote_volume=Decimal(100), taker_buy_quote_volume=Decimal(50)),
        _candle(8_639, quote_volume=Decimal(100), taker_buy_quote_volume=Decimal(100)),
        _candle(8_640, quote_volume=Decimal(0), taker_buy_quote_volume=Decimal(0)),
    )
    projection = _small_projection(rows)

    assert tuple(value.signed_share for value in projection.observed_slot_values) == (
        Decimal(-1),
        Decimal(0),
        Decimal(1),
        None,
    )
    assert tuple(value.signed_normal_notional for value in projection.observed_slot_values) == (
        Decimal(-100),
        Decimal(0),
        Decimal(100),
        Decimal(0),
    )


def test_source_and_economic_roots_have_distinct_domains() -> None:
    current = _base_rows()[-1]
    baseline = _small_projection((current,))
    source_only_candle = current.model_copy(update={"close": Decimal("100.5")})
    source_only = _small_projection((source_only_candle,))
    other_dataset = _small_projection((current,), dataset_sha256="b" * 64)
    economic_candle = current.model_copy(
        update={
            "quote_volume": current.quote_volume + Decimal(1),
        }
    )
    economic = _small_projection((economic_candle,))

    assert source_only.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert source_only.economic_flow_root_sha256 == baseline.economic_flow_root_sha256
    assert other_dataset.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert other_dataset.economic_flow_root_sha256 == baseline.economic_flow_root_sha256
    assert economic.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert economic.economic_flow_root_sha256 != baseline.economic_flow_root_sha256


def test_duplicate_conflict_range_and_identity_violations_fail_closed() -> None:
    current = _base_rows()[-1]
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="duplicate"):
        _small_projection((current, current))
    conflict = current.model_copy(update={"close": Decimal("100.5")})
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="conflicting"):
        _small_projection((current, conflict))
    outside = _candle(
        PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2,
    )
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="outside"):
        _small_projection((outside,))

    wrong_market = current.model_copy(update={"market": Market.SPOT})
    wrong_interval = current.model_copy(update={"interval": "1h"})
    unclosed = current.model_copy(update={"is_closed": False})
    for row, match in (
        (wrong_market, "futures"),
        (wrong_interval, "closed 5m"),
        (unclosed, "closed 5m"),
    ):
        with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match=match):
            _small_projection((row,))


def test_window_and_decimal_domain_boundaries_fail_closed() -> None:
    current = _base_rows()[-1]
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="aligned"):
        build_participation_historical_kline_proxy_v2(
            attempt_id=ATTEMPT_ID,
            dataset_sha256=DATASET_SHA256,
            bar_open_ms=CURRENT_BAR_OPEN_MS + 1,
            rows=(current,),
        )
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="Unix epoch"):
        build_participation_historical_kline_proxy_v2(
            attempt_id=ATTEMPT_ID,
            dataset_sha256=DATASET_SHA256,
            bar_open_ms=FIVE_MINUTE_MS_V2,
            rows=(current,),
        )

    nonfinite = current.model_copy(update={"quote_volume": Decimal("NaN")})
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="finite Decimal"):
        _small_projection((nonfinite,))
    huge = current.model_copy(
        update={
            "quote_volume": Decimal("1E+1000000"),
            "taker_buy_quote_volume": Decimal("1E+1000000"),
        }
    )
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="protocol bounds"):
        _small_projection((huge,))


def test_factory_and_post_mint_tampering_fail_closed() -> None:
    projection = _small_projection((_base_rows()[-1],))
    constructor_values = {
        item.name: getattr(projection, item.name)
        for item in fields(ParticipationHistoricalKlineProxyV2)
        if item.init
    }
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="factory"):
        ParticipationHistoricalKlineProxyV2(**constructor_values)  # type: ignore[arg-type]

    economic = projection.economic_flow_rows[0]
    economic_values = {
        item.name: getattr(economic, item.name)
        for item in fields(ParticipationHistoricalKlineEconomicEntryV2)
        if item.init
    }
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="factory"):
        ParticipationHistoricalKlineEconomicEntryV2(**economic_values)  # type: ignore[arg-type]

    object.__setattr__(projection, "producer_ready", True)
    with pytest.raises(ParticipationHistoricalKlineProxyContractErrorV2, match="authority"):
        canonical_participation_historical_kline_proxy_v2(projection)
