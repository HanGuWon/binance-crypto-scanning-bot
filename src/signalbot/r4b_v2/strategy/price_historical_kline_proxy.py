"""Outcome-blind historical-kline adapter for the frozen price calculation.

The adapter accepts only one exact chronological USD-M futures 5m window and
copies its closes into the already-frozen ``calculate_price_close_path_v2``
owner.  Historical dataset rows do not carry raw-capture membership, a strict
M1 parser receipt, or an M2 finality certificate, so numeric readiness is kept
separate from every live, promotion, probability, and target-return claim.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise
from typing import Final, Literal

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    PriceClosePathCalculationV2,
    PriceEvidenceContractErrorV2,
    calculate_price_close_path_v2,
    canonical_price_close_path_calculation_v2,
)

PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2: Final = (
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2
)
PRICE_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2: Final = "HISTORICAL_KLINE_PROXY_ONLY"
PRICE_HISTORICAL_KLINE_PROXY_ROLE_V2: Final = (
    "HISTORICAL_ONLY_NONPROMOTING_PRICE_CALCULATION_PROXY"
)

_SCHEMA_VERSION: Final = "r4b_price_historical_kline_proxy_v2"
_SOURCE_ENTRY_SCHEMA_VERSION: Final = "r4b_price_historical_kline_source_entry_v2"
_CLOSE_ENTRY_SCHEMA_VERSION: Final = "r4b_price_historical_kline_close_entry_v2"
_SOURCE_ROOT_SCHEMA_VERSION: Final = "r4b_price_historical_kline_source_root_v2"
_CLOSE_ROOT_SCHEMA_VERSION: Final = "r4b_price_historical_kline_close_root_v2"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_PRICE_HISTORICAL_KLINE_SOURCE_ROOT_V2\0"
_CLOSE_ROOT_DOMAIN: Final = b"R4B_PRICE_HISTORICAL_KLINE_CLOSE_ROOT_V2\0"
_PROXY_DOMAIN: Final = b"R4B_PRICE_HISTORICAL_KLINE_PROXY_V2\0"
_FACTORY_TOKEN: Final = object()
_ENTRY_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_REASON_RE: Final = re.compile(r"^[A-Z0-9_]+$")
_MAX_CANONICAL_INTEGER: Final = 2**53 - 1


class PriceHistoricalKlineProxyContractErrorV2(ValueError):
    """Raised when a historical price proxy would overstate its evidence."""


class PriceHistoricalKlineProxyStatusV2(StrEnum):
    """Numeric formula availability, never live or promotion readiness."""

    NUMERIC_READY_PROXY = "NUMERIC_READY_PROXY"
    NUMERIC_NONREADY_PROXY = "NUMERIC_NONREADY_PROXY"


@dataclass(frozen=True, slots=True)
class PriceHistoricalKlineSourceEntryV2:
    """All original fields copied from one accepted historical candle."""

    market: Market
    symbol: str
    interval: Literal["5m"]
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: Literal[True]
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price source entries require their proxy factory"
            )
        _validate_source_entry(self)


@dataclass(frozen=True, slots=True)
class PriceHistoricalKlineCloseEntryV2:
    """One close consumed by the frozen price formula."""

    bar_open_ms: int
    bar_close_ms: int
    close: Decimal
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price close entries require their proxy factory"
            )
        _validate_close_entry(self)


@dataclass(frozen=True, slots=True)
class PriceHistoricalKlineProxyV2:
    """Sealed retrospective price calculation without causal source authority."""

    attempt_id: str
    dataset_sha256: str
    symbol: str
    market: Market
    interval: Literal["5m"]
    bar_open_ms: int
    bar_close_ms: int
    expected_first_slot_open_ms: int
    historical_slice_through_ms: int
    ordered_source_rows: tuple[PriceHistoricalKlineSourceEntryV2, ...]
    economic_close_slice: tuple[PriceHistoricalKlineCloseEntryV2, ...]
    source_lineage_root_sha256: str
    economic_close_slice_sha256: str
    calculation: PriceClosePathCalculationV2
    status: PriceHistoricalKlineProxyStatusV2
    reasons: tuple[str, ...]
    direction: int
    strength_micros: int
    _factory_token: InitVar[object | None] = None
    proxy_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    )
    role: str = field(init=False, default=PRICE_HISTORICAL_KLINE_PROXY_ROLE_V2)
    authority_status: Literal["HISTORICAL_KLINE_PROXY_ONLY"] = field(
        init=False,
        default="HISTORICAL_KLINE_PROXY_ONLY",
    )
    expected_row_count: int = field(
        init=False,
        default=PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2,
    )
    historical_only: Literal[True] = field(init=False, default=True)
    historical_diagnostic_only: Literal[True] = field(init=False, default=True)
    outcome_data_read: Literal[False] = field(init=False, default=False)
    target_return_used: Literal[False] = field(init=False, default=False)
    exact_kline_m1_equivalent: Literal[False] = field(init=False, default=False)
    verified_raw_membership_m0_bound: Literal[False] = field(init=False, default=False)
    strict_source_parser_m1_bound: Literal[False] = field(init=False, default=False)
    causal_cursor_finality_m2_bound: Literal[False] = field(init=False, default=False)
    causal_inputs_complete: Literal[False] = field(init=False, default=False)
    producer_ready: Literal[False] = field(init=False, default=False)
    promoting_eligible: Literal[False] = field(init=False, default=False)
    probability_eligible: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    target_return_eligible: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)
    m0_root_sha256: None = field(init=False, default=None)
    m1_payload_sha256: None = field(init=False, default=None)
    m2_certificate_sha256: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price proxies require their canonical factory"
            )
        _validate_proxy(self)
        object.__setattr__(
            self,
            "proxy_sha256",
            hashlib.sha256(
                _PROXY_DOMAIN
                + canonical_json_line(_proxy_document(self, include_proxy_hash=False))
            ).hexdigest(),
        )

    @property
    def calculation_ready(self) -> bool:
        """Return arithmetic readiness without implying producer readiness."""

        return self.calculation.ready


def build_price_historical_kline_proxy_v2(
    *,
    attempt_id: str,
    dataset_sha256: str,
    bar_open_ms: int,
    rows: tuple[Candle, ...],
) -> PriceHistoricalKlineProxyV2:
    """Build an exact, ordered 8,653-close retrospective calculation.

    ``dataset_sha256`` is a caller-supplied provenance claim and is bound into
    the source root.  This pure adapter does not read files, outcomes, labels,
    forward returns, fills, or any row after ``bar_open_ms``.
    """

    _validate_identity(attempt_id, "attempt_id")
    _validate_sha256(dataset_sha256, "dataset_sha256")
    _validate_decision_slot(bar_open_ms)
    if type(rows) is not tuple or len(rows) != PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy requires exactly 8,653 immutable rows"
        )
    if any(not isinstance(row, Candle) for row in rows):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy contains a non-Candle row"
        )

    expected_first_slot_open_ms = bar_open_ms - (
        (PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
    )
    if expected_first_slot_open_ms < 0:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy window would precede Unix epoch"
        )
    _validate_ordered_candles(
        rows,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        bar_open_ms=bar_open_ms,
    )
    source_rows = tuple(_source_entry(row) for row in rows)
    close_slice = tuple(_close_entry_from_source(row) for row in source_rows)
    try:
        calculation = calculate_price_close_path_v2(tuple(row.close for row in close_slice))
    except PriceEvidenceContractErrorV2 as exc:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price close slice violates the frozen calculation contract"
        ) from exc
    status = _proxy_status(calculation)
    reasons = _proxy_reasons(calculation)
    current = source_rows[-1]
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    source_root = _source_root(
        source_rows,
        attempt_id=attempt_id,
        dataset_sha256=dataset_sha256,
        symbol=current.symbol,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
    )
    close_root = _close_root(
        close_slice,
        symbol=current.symbol,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
    )
    return PriceHistoricalKlineProxyV2(
        attempt_id=attempt_id,
        dataset_sha256=dataset_sha256,
        symbol=current.symbol,
        market=Market.FUTURES,
        interval="5m",
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        historical_slice_through_ms=bar_close_ms,
        ordered_source_rows=source_rows,
        economic_close_slice=close_slice,
        source_lineage_root_sha256=source_root,
        economic_close_slice_sha256=close_root,
        calculation=calculation,
        status=status,
        reasons=reasons,
        direction=calculation.direction,
        strength_micros=calculation.strength_micros,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_price_historical_kline_proxy_v2(
    value: PriceHistoricalKlineProxyV2,
) -> bytes:
    """Serialize and live-validate a sealed historical-only price proxy."""

    if not isinstance(value, PriceHistoricalKlineProxyV2):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "value must be PriceHistoricalKlineProxyV2"
        )
    _validate_proxy(value)
    expected_close_slice = tuple(
        _close_entry_from_source(row) for row in value.ordered_source_rows
    )
    if value.economic_close_slice != expected_close_slice:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price close slice differs from sealed source rows"
        )
    try:
        expected_calculation = calculate_price_close_path_v2(
            tuple(row.close for row in expected_close_slice)
        )
    except PriceEvidenceContractErrorV2 as exc:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price close slice violates the frozen calculation contract"
        ) from exc
    if (
        value.calculation != expected_calculation
        or value.status is not _proxy_status(expected_calculation)
        or value.reasons != _proxy_reasons(expected_calculation)
        or value.direction != expected_calculation.direction
        or value.strength_micros != expected_calculation.strength_micros
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price status or numeric result differs from its exact close slice"
        )
    expected_source_root = _source_root(
        value.ordered_source_rows,
        attempt_id=value.attempt_id,
        dataset_sha256=value.dataset_sha256,
        symbol=value.symbol,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        expected_first_slot_open_ms=value.expected_first_slot_open_ms,
    )
    if value.source_lineage_root_sha256 != expected_source_root:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source rows differ from their sealed root"
        )
    expected_close_root = _close_root(
        value.economic_close_slice,
        symbol=value.symbol,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        expected_first_slot_open_ms=value.expected_first_slot_open_ms,
    )
    if value.economic_close_slice_sha256 != expected_close_root:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price close slice differs from its sealed root"
        )
    expected_proxy_hash = hashlib.sha256(
        _PROXY_DOMAIN + canonical_json_line(_proxy_document(value, include_proxy_hash=False))
    ).hexdigest()
    if value.proxy_sha256 != expected_proxy_hash:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy differs from canonical content"
        )
    return canonical_json_line(_proxy_document(value, include_proxy_hash=True))


def _validate_ordered_candles(
    rows: tuple[Candle, ...],
    *,
    expected_first_slot_open_ms: int,
    bar_open_ms: int,
) -> None:
    first = rows[0]
    identity = (first.market, first.symbol, first.interval)
    for row in rows:
        _validate_candle(row)
        if (row.market, row.symbol, row.interval) != identity:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price rows must share one market, symbol, and interval"
            )
        if row.open_time_ms > bar_open_ms:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price proxy cannot consume a future row"
            )
    open_times = tuple(row.open_time_ms for row in rows)
    if len(set(open_times)) != len(open_times):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy contains a duplicate candle slot"
        )
    if open_times != tuple(sorted(open_times)):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price candle order must be strictly chronological"
        )
    for index, row in enumerate(rows):
        expected_open_ms = expected_first_slot_open_ms + index * FIVE_MINUTE_MS_V2
        if row.open_time_ms != expected_open_ms:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price candle gap is not exact and contiguous"
            )
    if rows[-1].open_time_ms != bar_open_ms:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price rows must end at the decision bar"
        )


def _validate_candle(value: Candle) -> None:
    if value.market is not Market.FUTURES:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy requires USD-M futures klines"
        )
    _validate_symbol(value.symbol)
    if value.interval != "5m" or value.is_closed is not True:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy requires fully closed 5m klines"
        )
    _validate_slot(value.open_time_ms, value.close_time_ms)
    for number, name in (
        (value.open, "open"),
        (value.high, "high"),
        (value.low, "low"),
        (value.close, "close"),
        (value.volume, "volume"),
        (value.quote_volume, "quote_volume"),
        (value.taker_buy_base_volume, "taker_buy_base_volume"),
        (value.taker_buy_quote_volume, "taker_buy_quote_volume"),
    ):
        _validate_decimal(number, name)
    if min(value.open, value.high, value.low, value.close) <= 0:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price candle prices and close must be positive"
        )
    if min(
        value.volume,
        value.quote_volume,
        value.taker_buy_base_volume,
        value.taker_buy_quote_volume,
    ) < 0:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price candle volumes must be nonnegative"
        )
    if (
        value.high < max(value.open, value.close)
        or value.low > min(value.open, value.close)
        or value.low > value.high
        or value.taker_buy_base_volume > value.volume
        or value.taker_buy_quote_volume > value.quote_volume
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price candle fields are inconsistent"
        )
    if type(value.trade_count) is not int or not 0 <= value.trade_count <= _MAX_CANONICAL_INTEGER:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price trade_count is outside canonical bounds"
        )


def _source_entry(value: Candle) -> PriceHistoricalKlineSourceEntryV2:
    return PriceHistoricalKlineSourceEntryV2(
        market=value.market,
        symbol=value.symbol,
        interval="5m",
        open_time_ms=value.open_time_ms,
        close_time_ms=value.close_time_ms,
        open=value.open,
        high=value.high,
        low=value.low,
        close=value.close,
        volume=value.volume,
        quote_volume=value.quote_volume,
        trade_count=value.trade_count,
        taker_buy_base_volume=value.taker_buy_base_volume,
        taker_buy_quote_volume=value.taker_buy_quote_volume,
        is_closed=True,
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _close_entry_from_source(
    value: PriceHistoricalKlineSourceEntryV2,
) -> PriceHistoricalKlineCloseEntryV2:
    _validate_source_entry(value)
    return PriceHistoricalKlineCloseEntryV2(
        bar_open_ms=value.open_time_ms,
        bar_close_ms=value.close_time_ms,
        close=value.close,
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _validate_source_entry(value: PriceHistoricalKlineSourceEntryV2) -> None:
    if value.market is not Market.FUTURES or value.interval != "5m" or value.is_closed is not True:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source identity is not exact"
        )
    _validate_symbol(value.symbol)
    _validate_slot(value.open_time_ms, value.close_time_ms)
    for number, name in (
        (value.open, "open"),
        (value.high, "high"),
        (value.low, "low"),
        (value.close, "close"),
        (value.volume, "volume"),
        (value.quote_volume, "quote_volume"),
        (value.taker_buy_base_volume, "taker_buy_base_volume"),
        (value.taker_buy_quote_volume, "taker_buy_quote_volume"),
    ):
        _validate_decimal(number, name)
    if min(value.open, value.high, value.low, value.close) <= 0:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source prices and close must be positive"
        )
    if min(
        value.volume,
        value.quote_volume,
        value.taker_buy_base_volume,
        value.taker_buy_quote_volume,
    ) < 0:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source volumes must be nonnegative"
        )
    if (
        value.high < max(value.open, value.close)
        or value.low > min(value.open, value.close)
        or value.low > value.high
        or value.taker_buy_base_volume > value.volume
        or value.taker_buy_quote_volume > value.quote_volume
        or type(value.trade_count) is not int
        or not 0 <= value.trade_count <= _MAX_CANONICAL_INTEGER
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source fields are inconsistent"
        )


def _validate_close_entry(value: PriceHistoricalKlineCloseEntryV2) -> None:
    _validate_slot(value.bar_open_ms, value.bar_close_ms)
    if not _is_positive_decimal(value.close):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price close entry must be a positive finite Decimal"
        )


def _validate_proxy(value: PriceHistoricalKlineProxyV2) -> None:
    if value.schema_version != _SCHEMA_VERSION:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "unsupported historical price proxy schema"
        )
    _validate_identity(value.attempt_id, "attempt_id")
    _validate_sha256(value.dataset_sha256, "dataset_sha256")
    _validate_symbol(value.symbol)
    _validate_decision_slot(value.bar_open_ms)
    expected_close_ms = value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    expected_first_ms = value.bar_open_ms - (
        (PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
    )
    if (
        value.market is not Market.FUTURES
        or value.interval != "5m"
        or value.bar_close_ms != expected_close_ms
        or value.expected_first_slot_open_ms != expected_first_ms
        or value.historical_slice_through_ms != expected_close_ms
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy identity or exact range differs"
        )
    if (
        value.rule_version != PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2
        or value.role != PRICE_HISTORICAL_KLINE_PROXY_ROLE_V2
        or value.authority_status != PRICE_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2
        or value.expected_row_count != PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy protocol identity differs"
        )
    if (
        value.historical_only is not True
        or value.historical_diagnostic_only is not True
        or value.outcome_data_read is not False
        or value.target_return_used is not False
        or value.exact_kline_m1_equivalent is not False
        or value.verified_raw_membership_m0_bound is not False
        or value.strict_source_parser_m1_bound is not False
        or value.causal_cursor_finality_m2_bound is not False
        or value.causal_inputs_complete is not False
        or value.producer_ready is not False
        or value.promoting_eligible is not False
        or value.probability_eligible is not False
        or value.probability_calibrated is not False
        or value.target_return_eligible is not False
        or value.data_through_ms is not None
        or value.m0_root_sha256 is not None
        or value.m1_payload_sha256 is not None
        or value.m2_certificate_sha256 is not None
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price authority and downstream claims must remain false"
        )
    if (
        type(value.ordered_source_rows) is not tuple
        or len(value.ordered_source_rows) != PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2
        or type(value.economic_close_slice) is not tuple
        or len(value.economic_close_slice) != PRICE_HISTORICAL_KLINE_PROXY_EXPECTED_ROW_COUNT_V2
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy row collections differ from the exact window"
        )
    if any(
        not isinstance(row, PriceHistoricalKlineSourceEntryV2)
        for row in value.ordered_source_rows
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy contains an unsupported source row"
        )
    if any(
        not isinstance(row, PriceHistoricalKlineCloseEntryV2)
        for row in value.economic_close_slice
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy contains an unsupported close row"
        )
    _validate_ordered_source_entries(
        value.ordered_source_rows,
        symbol=value.symbol,
        expected_first_slot_open_ms=value.expected_first_slot_open_ms,
        bar_open_ms=value.bar_open_ms,
    )
    for row in value.economic_close_slice:
        _validate_close_entry(row)
    _validate_sha256(value.source_lineage_root_sha256, "source_lineage_root_sha256")
    _validate_sha256(value.economic_close_slice_sha256, "economic_close_slice_sha256")
    try:
        canonical_price_close_path_calculation_v2(value.calculation)
    except PriceEvidenceContractErrorV2 as exc:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy calculation is not canonical"
        ) from exc
    if not isinstance(value.status, PriceHistoricalKlineProxyStatusV2):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy status enum differs"
        )
    _validate_reasons(value.reasons)
    if (
        value.status is not _proxy_status(value.calculation)
        or value.direction != value.calculation.direction
        or value.strength_micros != value.calculation.strength_micros
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy numeric summary differs from its calculation"
        )


def _validate_ordered_source_entries(
    rows: tuple[PriceHistoricalKlineSourceEntryV2, ...],
    *,
    symbol: str,
    expected_first_slot_open_ms: int,
    bar_open_ms: int,
) -> None:
    for index, row in enumerate(rows):
        _validate_source_entry(row)
        expected_open_ms = expected_first_slot_open_ms + index * FIVE_MINUTE_MS_V2
        if row.symbol != symbol or row.open_time_ms != expected_open_ms:
            raise PriceHistoricalKlineProxyContractErrorV2(
                "historical price source order, identity, or continuity differs"
            )
    if rows[-1].open_time_ms != bar_open_ms or any(
        current.open_time_ms != previous.open_time_ms + FIVE_MINUTE_MS_V2
        for previous, current in pairwise(rows)
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price source rows are not exact and contiguous"
        )


def _proxy_status(
    calculation: PriceClosePathCalculationV2,
) -> PriceHistoricalKlineProxyStatusV2:
    if calculation.ready:
        return PriceHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY
    return PriceHistoricalKlineProxyStatusV2.NUMERIC_NONREADY_PROXY


def _proxy_reasons(calculation: PriceClosePathCalculationV2) -> tuple[str, ...]:
    return (
        "EXACT_8653_CLOSED_ORDERED_HISTORICAL_FUTURES_KLINES",
        calculation.reason,
        "M0_M1_M2_SOURCE_AUTHORITY_UNBOUND",
        "NUMERIC_STATUS_HAS_NO_PRODUCER_PROMOTION_PROBABILITY_OR_TARGET_RETURN_AUTHORITY",
    )


def _source_root(
    rows: tuple[PriceHistoricalKlineSourceEntryV2, ...],
    *,
    attempt_id: str,
    dataset_sha256: str,
    symbol: str,
    bar_open_ms: int,
    bar_close_ms: int,
    expected_first_slot_open_ms: int,
) -> str:
    return hashlib.sha256(
        _SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": attempt_id,
                "authority_status": PRICE_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2,
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "dataset_sha256": dataset_sha256,
                "expected_first_slot_open_ms": expected_first_slot_open_ms,
                "historical_only": True,
                "ordered_rows": [_source_document(row) for row in rows],
                "outcome_data_read": False,
                "row_count": len(rows),
                "schema_version": _SOURCE_ROOT_SCHEMA_VERSION,
                "symbol": symbol,
                "target_return_used": False,
            }
        )
    ).hexdigest()


def _close_root(
    rows: tuple[PriceHistoricalKlineCloseEntryV2, ...],
    *,
    symbol: str,
    bar_open_ms: int,
    bar_close_ms: int,
    expected_first_slot_open_ms: int,
) -> str:
    return hashlib.sha256(
        _CLOSE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "expected_first_slot_open_ms": expected_first_slot_open_ms,
                "rows": [_close_document(row) for row in rows],
                "rule_version": PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
                "schema_version": _CLOSE_ROOT_SCHEMA_VERSION,
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _proxy_document(
    value: PriceHistoricalKlineProxyV2,
    *,
    include_proxy_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": value.attempt_id,
        "authority_status": value.authority_status,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "calculation": _calculation_document(value.calculation),
        "causal_cursor_finality_m2_bound": value.causal_cursor_finality_m2_bound,
        "causal_inputs_complete": value.causal_inputs_complete,
        "data_through_ms": value.data_through_ms,
        "dataset_sha256": value.dataset_sha256,
        "direction": value.direction,
        "economic_close_slice": [_close_document(row) for row in value.economic_close_slice],
        "economic_close_slice_sha256": value.economic_close_slice_sha256,
        "exact_kline_m1_equivalent": value.exact_kline_m1_equivalent,
        "expected_first_slot_open_ms": value.expected_first_slot_open_ms,
        "expected_row_count": value.expected_row_count,
        "historical_diagnostic_only": value.historical_diagnostic_only,
        "historical_only": value.historical_only,
        "historical_slice_through_ms": value.historical_slice_through_ms,
        "interval": value.interval,
        "m0_root_sha256": value.m0_root_sha256,
        "m1_payload_sha256": value.m1_payload_sha256,
        "m2_certificate_sha256": value.m2_certificate_sha256,
        "market": value.market.value,
        "ordered_source_rows": [_source_document(row) for row in value.ordered_source_rows],
        "outcome_data_read": value.outcome_data_read,
        "probability_calibrated": value.probability_calibrated,
        "probability_eligible": value.probability_eligible,
        "producer_ready": value.producer_ready,
        "promoting_eligible": value.promoting_eligible,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "strict_source_parser_m1_bound": value.strict_source_parser_m1_bound,
        "symbol": value.symbol,
        "target_return_eligible": value.target_return_eligible,
        "target_return_used": value.target_return_used,
        "verified_raw_membership_m0_bound": value.verified_raw_membership_m0_bound,
    }
    if include_proxy_hash:
        document["proxy_sha256"] = value.proxy_sha256
    return document


def _source_document(value: PriceHistoricalKlineSourceEntryV2) -> dict[str, object]:
    return {
        "close": str(value.close),
        "close_time_ms": value.close_time_ms,
        "high": str(value.high),
        "interval": value.interval,
        "is_closed": value.is_closed,
        "low": str(value.low),
        "market": value.market.value,
        "open": str(value.open),
        "open_time_ms": value.open_time_ms,
        "quote_volume": str(value.quote_volume),
        "schema_version": _SOURCE_ENTRY_SCHEMA_VERSION,
        "symbol": value.symbol,
        "taker_buy_base_volume": str(value.taker_buy_base_volume),
        "taker_buy_quote_volume": str(value.taker_buy_quote_volume),
        "trade_count": value.trade_count,
        "volume": str(value.volume),
    }


def _close_document(value: PriceHistoricalKlineCloseEntryV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close": str(value.close),
        "schema_version": _CLOSE_ENTRY_SCHEMA_VERSION,
    }


def _calculation_document(value: PriceClosePathCalculationV2) -> dict[str, object]:
    canonical_price_close_path_calculation_v2(value)
    return {
        "calculation_sha256": value.calculation_sha256,
        "composite": _decimal_or_none(value.composite),
        "current_return_1": _decimal_or_none(value.current_return_1),
        "current_return_12": _decimal_or_none(value.current_return_12),
        "direction": value.direction,
        "normalized_return_1": _decimal_or_none(value.normalized_return_1),
        "normalized_return_12": _decimal_or_none(value.normalized_return_12),
        "prior_location_1": _decimal_or_none(value.prior_location_1),
        "prior_location_12": _decimal_or_none(value.prior_location_12),
        "prior_mad_1": _decimal_or_none(value.prior_mad_1),
        "prior_mad_12": _decimal_or_none(value.prior_mad_12),
        "prior_observation_count": value.prior_observation_count,
        "prior_scale_1": _decimal_or_none(value.prior_scale_1),
        "prior_scale_12": _decimal_or_none(value.prior_scale_12),
        "reason": value.reason,
        "rule_version": value.rule_version,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }


def _validate_decision_slot(value: int) -> None:
    if (
        type(value) is not int
        or value < 0
        or value > _MAX_CANONICAL_INTEGER - FIVE_MINUTE_MS_V2
        or value % FIVE_MINUTE_MS_V2 != 0
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price decision bar must be an aligned canonical 5m slot"
        )


def _validate_slot(open_ms: int, close_ms: int) -> None:
    if (
        type(open_ms) is not int
        or type(close_ms) is not int
        or open_ms < 0
        or close_ms > _MAX_CANONICAL_INTEGER
        or open_ms % FIVE_MINUTE_MS_V2 != 0
        or close_ms != open_ms + FIVE_MINUTE_MS_V2 - 1
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price kline must bind one exact aligned 5m slot"
        )


def _validate_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise PriceHistoricalKlineProxyContractErrorV2(
            f"historical price {name} must be a finite Decimal"
        )


def _validate_identity(value: str, name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 256
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            f"{name} must be nonempty bounded canonical text"
        )


def _validate_symbol(value: str) -> None:
    if type(value) is not str or _SYMBOL_RE.fullmatch(value) is None:
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price symbol must be canonical uppercase USDT"
        )


def _validate_sha256(value: str, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise PriceHistoricalKlineProxyContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if (
        type(values) is not tuple
        or len(values) != 4
        or len(set(values)) != len(values)
        or any(
            type(value) is not str
            or _REASON_RE.fullmatch(value) is None
            or len(value) > 256
            for value in values
        )
    ):
        raise PriceHistoricalKlineProxyContractErrorV2(
            "historical price proxy reasons must be exact canonical codes"
        )


def _is_positive_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
