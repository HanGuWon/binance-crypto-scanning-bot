"""Outcome-blind historical-kline proxy for the frozen participation formula.

This adapter is deliberately weaker than the exact aggregate-trade M1 path.  It
uses only closed historical 5m kline ``quote_volume`` and
``taker_buy_quote_volume`` fields and seals the explicit assumption that every
trade was normal.  Numeric calculation readiness never implies source,
producer, promotion, or probability authority.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException, localcontext
from enum import StrEnum
from itertools import pairwise
from typing import Final, Literal

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import ROBUST_Z_PRIOR_WINDOW_V2
from signalbot.r4b_v2.strategy.participation_evidence import (
    PARTICIPATION_FLOW_RULE_VERSION_V2,
    ParticipationFlowBarValueV2,
    ParticipationFlowCalculationV2,
    ParticipationFlowContractErrorV2,
    build_participation_flow_bar_value_v2,
    calculate_participation_flow_v2,
    canonical_participation_flow_bar_value_v2,
    canonical_participation_flow_calculation_v2,
)

PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 1
PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2: Final = "ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY"
PARTICIPATION_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2: Final = "HISTORICAL_KLINE_PROXY_ONLY"

_SCHEMA_VERSION: Final = "r4b_participation_historical_kline_proxy_v2"
_SOURCE_ENTRY_SCHEMA: Final = "r4b_participation_historical_kline_source_entry_v2"
_ECONOMIC_ENTRY_SCHEMA: Final = "r4b_participation_historical_kline_economic_entry_v2"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_HISTORICAL_KLINE_SOURCE_ROOT_V2\0"
_ECONOMIC_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_HISTORICAL_KLINE_ECONOMIC_ROOT_V2\0"
_PROJECTION_DOMAIN: Final = b"R4B_PARTICIPATION_HISTORICAL_KLINE_PROXY_V2\0"
_FACTORY_TOKEN: Final = object()
_ENTRY_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_REASON_RE: Final = re.compile(r"^[A-Z0-9_]+$")
_MAX_CANONICAL_INTEGER: Final = 2**53 - 1


class ParticipationHistoricalKlineProxyContractErrorV2(ValueError):
    """Raised when a historical-kline proxy would overstate its evidence."""


class ParticipationHistoricalKlineProxyStatusV2(StrEnum):
    """Availability of the retrospective numeric proxy, not live readiness."""

    NUMERIC_READY_PROXY = "NUMERIC_READY_PROXY"
    NUMERIC_NONREADY_PROXY = "NUMERIC_NONREADY_PROXY"
    UNAVAILABLE_MISSING_SLOT_UNKNOWN = "UNAVAILABLE_MISSING_SLOT_UNKNOWN"
    UNAVAILABLE_INTERNAL_GAP_UNKNOWN = "UNAVAILABLE_INTERNAL_GAP_UNKNOWN"


@dataclass(frozen=True, slots=True)
class ParticipationHistoricalKlineSourceEntryV2:
    """Every source field retained from one closed historical 5m kline."""

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
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical kline source entries require their proxy factory"
            )
        _validate_source_entry(self)


@dataclass(frozen=True, slots=True)
class ParticipationHistoricalKlineEconomicEntryV2:
    """The two kline fields and exact proxy derivations used economically."""

    bar_open_ms: int
    bar_close_ms: int
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal
    signed_normal_notional: Decimal
    normal_notional: Decimal
    total_trade_notional: Decimal
    signed_share: Decimal | None
    proxy_assumption: Literal["ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY"]
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_FACTORY_TOKEN:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical kline economic entries require their proxy factory"
            )
        _validate_economic_entry(self)


@dataclass(frozen=True, slots=True)
class ParticipationHistoricalKlineProxyV2:
    """Retrospective kline calculation with all live authority sealed false."""

    attempt_id: str
    dataset_sha256: str
    symbol: str
    market: Market
    interval: Literal["5m"]
    bar_open_ms: int
    bar_close_ms: int
    expected_first_slot_open_ms: int
    historical_slice_through_ms: int
    ordered_source_rows: tuple[ParticipationHistoricalKlineSourceEntryV2, ...]
    economic_flow_rows: tuple[ParticipationHistoricalKlineEconomicEntryV2, ...]
    observed_slot_values: tuple[ParticipationFlowBarValueV2, ...]
    missing_slot_open_ms: tuple[int, ...]
    internal_gap_after_open_ms: tuple[int, ...]
    source_lineage_root_sha256: str
    economic_flow_root_sha256: str
    calculation: ParticipationFlowCalculationV2 | None
    status: ParticipationHistoricalKlineProxyStatusV2
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    projection_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(init=False, default=PARTICIPATION_FLOW_RULE_VERSION_V2)
    proxy_assumption: Literal["ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY"] = field(
        init=False,
        default="ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY",
    )
    authority_status: Literal["HISTORICAL_KLINE_PROXY_ONLY"] = field(
        init=False,
        default="HISTORICAL_KLINE_PROXY_ONLY",
    )
    slot_absence_interpretation: Literal["UNKNOWN_NOT_ZERO"] = field(
        init=False,
        default="UNKNOWN_NOT_ZERO",
    )
    expected_slot_count: int = field(
        init=False,
        default=PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2,
    )
    historical_diagnostic_only: Literal[True] = field(init=False, default=True)
    outcome_data_read: Literal[False] = field(init=False, default=False)
    exact_agg_trade_m1_equivalent: Literal[False] = field(init=False, default=False)
    verified_raw_membership_m0_bound: Literal[False] = field(init=False, default=False)
    strict_source_parser_m1_bound: Literal[False] = field(init=False, default=False)
    causal_cursor_finality_m2_bound: Literal[False] = field(init=False, default=False)
    causal_inputs_complete: Literal[False] = field(init=False, default=False)
    producer_ready: Literal[False] = field(init=False, default=False)
    promoting_eligible: Literal[False] = field(init=False, default=False)
    probability_eligible: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)
    m0_root_sha256: None = field(init=False, default=None)
    m1_payload_sha256: None = field(init=False, default=None)
    m2_certificate_sha256: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical kline proxies require their canonical factory"
            )
        _validate_projection(self)
        object.__setattr__(
            self,
            "projection_sha256",
            hashlib.sha256(
                _PROJECTION_DOMAIN
                + canonical_json_line(_projection_document(self, include_projection_hash=False))
            ).hexdigest(),
        )

    @property
    def calculation_ready(self) -> bool:
        """Report numeric formula readiness without granting live authority."""

        return self.calculation is not None and self.calculation.ready

    @property
    def all_expected_slots_observed(self) -> bool:
        return not self.missing_slot_open_ms


def build_participation_historical_kline_proxy_v2(
    *,
    attempt_id: str,
    dataset_sha256: str,
    bar_open_ms: int,
    rows: tuple[Candle, ...],
) -> ParticipationHistoricalKlineProxyV2:
    """Adapt closed futures klines without reading labels, fills, or outcomes.

    ``dataset_sha256`` is provenance supplied by a caller that has verified the
    dataset manifest.  This source-neutral strategy adapter binds the claim into
    its source root; it performs no filesystem access itself.
    """

    _validate_identity(attempt_id, "attempt_id")
    _validate_sha256(dataset_sha256, "dataset_sha256")
    _validate_decision_slot(bar_open_ms)
    if type(rows) is not tuple or not rows:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline rows must be a non-empty immutable tuple"
        )
    if any(not isinstance(row, Candle) for row in rows):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy contains a non-Candle row"
        )

    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    expected_first_slot_open_ms = bar_open_ms - (ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2)
    if expected_first_slot_open_ms < 0:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy window would precede Unix epoch"
        )
    ordered = _validate_and_order_candles(
        rows,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        bar_open_ms=bar_open_ms,
    )
    source_rows = tuple(_source_entry(row) for row in ordered)
    economic_rows = tuple(_economic_entry_from_source(row) for row in source_rows)
    observed_values = tuple(_bar_value_from_economic(row) for row in economic_rows)
    expected_slots = _expected_slots(expected_first_slot_open_ms)
    observed_slots = {row.bar_open_ms for row in economic_rows}
    missing_slots = tuple(slot for slot in expected_slots if slot not in observed_slots)
    gaps = _internal_gap_after(source_rows)
    calculation: ParticipationFlowCalculationV2 | None = None
    if not missing_slots:
        calculation = calculate_participation_flow_v2(
            current_bar=observed_values[-1],
            prior_bars=observed_values[:-1],
        )
    status = _projection_status(
        missing_slots=missing_slots,
        internal_gaps=gaps,
        calculation=calculation,
    )
    reasons = _projection_reasons(status, calculation)
    symbol = source_rows[0].symbol
    source_root = _source_root(
        source_rows,
        attempt_id=attempt_id,
        dataset_sha256=dataset_sha256,
        symbol=symbol,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
    )
    economic_root = _economic_root(
        economic_rows,
        observed_values,
        symbol=symbol,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
    )
    return ParticipationHistoricalKlineProxyV2(
        attempt_id=attempt_id,
        dataset_sha256=dataset_sha256,
        symbol=symbol,
        market=Market.FUTURES,
        interval="5m",
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        expected_first_slot_open_ms=expected_first_slot_open_ms,
        historical_slice_through_ms=bar_close_ms,
        ordered_source_rows=source_rows,
        economic_flow_rows=economic_rows,
        observed_slot_values=observed_values,
        missing_slot_open_ms=missing_slots,
        internal_gap_after_open_ms=gaps,
        source_lineage_root_sha256=source_root,
        economic_flow_root_sha256=economic_root,
        calculation=calculation,
        status=status,
        reasons=reasons,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_participation_historical_kline_proxy_v2(
    value: ParticipationHistoricalKlineProxyV2,
) -> bytes:
    """Serialize and live-check one sealed outcome-blind kline proxy."""

    if not isinstance(value, ParticipationHistoricalKlineProxyV2):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "value must be ParticipationHistoricalKlineProxyV2"
        )
    _validate_projection(value)
    expected_economic = tuple(_economic_entry_from_source(row) for row in value.ordered_source_rows)
    if value.economic_flow_rows != expected_economic:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline economic rows differ from quote-volume source fields"
        )
    expected_values = tuple(_bar_value_from_economic(row) for row in expected_economic)
    if value.observed_slot_values != expected_values:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline slot values differ from sealed proxy economics"
        )
    expected_slots = _expected_slots(value.expected_first_slot_open_ms)
    observed_slots = {row.bar_open_ms for row in expected_economic}
    expected_missing = tuple(slot for slot in expected_slots if slot not in observed_slots)
    expected_gaps = _internal_gap_after(value.ordered_source_rows)
    if (
        value.missing_slot_open_ms != expected_missing
        or value.internal_gap_after_open_ms != expected_gaps
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline missing/gap claims differ from observed slots"
        )
    expected_calculation: ParticipationFlowCalculationV2 | None = None
    if not expected_missing:
        expected_calculation = calculate_participation_flow_v2(
            current_bar=expected_values[-1],
            prior_bars=expected_values[:-1],
        )
    expected_status = _projection_status(
        missing_slots=expected_missing,
        internal_gaps=expected_gaps,
        calculation=expected_calculation,
    )
    expected_reasons = _projection_reasons(expected_status, expected_calculation)
    if (
        value.calculation != expected_calculation
        or value.status is not expected_status
        or value.reasons != expected_reasons
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline status or calculation differs from its exact slice"
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
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline source slice differs from its sealed root"
        )
    expected_economic_root = _economic_root(
        value.economic_flow_rows,
        value.observed_slot_values,
        symbol=value.symbol,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        expected_first_slot_open_ms=value.expected_first_slot_open_ms,
    )
    if value.economic_flow_root_sha256 != expected_economic_root:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline economic slice differs from its sealed root"
        )
    expected_projection = hashlib.sha256(
        _PROJECTION_DOMAIN
        + canonical_json_line(_projection_document(value, include_projection_hash=False))
    ).hexdigest()
    if value.projection_sha256 != expected_projection:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy differs from canonical content"
        )
    return canonical_json_line(_projection_document(value, include_projection_hash=True))


def _validate_and_order_candles(
    rows: tuple[Candle, ...],
    *,
    expected_first_slot_open_ms: int,
    bar_open_ms: int,
) -> tuple[Candle, ...]:
    seen: dict[int, Candle] = {}
    identity: tuple[Market, str, str] | None = None
    for row in rows:
        _validate_candle(row)
        row_identity = (row.market, row.symbol, row.interval)
        if identity is None:
            identity = row_identity
        elif row_identity != identity:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical kline rows must share one market, symbol, and interval"
            )
        if not expected_first_slot_open_ms <= row.open_time_ms <= bar_open_ms:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical kline row lies outside the exact 8,641-slot window"
            )
        existing = seen.get(row.open_time_ms)
        if existing is not None:
            qualifier = "duplicate" if existing == row else "conflicting"
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                f"historical kline {qualifier} slot at {row.open_time_ms}"
            )
        seen[row.open_time_ms] = row
    return tuple(seen[key] for key in sorted(seen))


def _validate_candle(value: Candle) -> None:
    if value.market is not Market.FUTURES:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical participation proxy requires futures klines"
        )
    _validate_symbol(value.symbol)
    if value.interval != "5m" or value.is_closed is not True:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical participation proxy requires fully closed 5m klines"
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
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline prices must be positive"
        )
    if (
        min(
            value.volume,
            value.quote_volume,
            value.taker_buy_base_volume,
            value.taker_buy_quote_volume,
        )
        < 0
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline volumes must be nonnegative"
        )
    if (
        value.high < max(value.open, value.close)
        or value.low > min(value.open, value.close)
        or value.low > value.high
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline OHLC values are inconsistent"
        )
    if (
        value.taker_buy_base_volume > value.volume
        or value.taker_buy_quote_volume > value.quote_volume
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline taker-buy volume exceeds total volume"
        )
    if type(value.trade_count) is not int or not 0 <= value.trade_count <= _MAX_CANONICAL_INTEGER:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline trade_count is outside canonical bounds"
        )


def _source_entry(value: Candle) -> ParticipationHistoricalKlineSourceEntryV2:
    return ParticipationHistoricalKlineSourceEntryV2(
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


def _economic_entry_from_source(
    value: ParticipationHistoricalKlineSourceEntryV2,
) -> ParticipationHistoricalKlineEconomicEntryV2:
    _validate_source_entry(value)
    try:
        with localcontext(protocol_decimal_context_v2()):
            total = +value.quote_volume
            normal = +value.quote_volume
            signed = Decimal(2) * value.taker_buy_quote_volume - value.quote_volume
            share = signed / total if total > 0 else None
    except DecimalException as exc:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy Decimal arithmetic is outside protocol bounds"
        ) from exc
    return ParticipationHistoricalKlineEconomicEntryV2(
        bar_open_ms=value.open_time_ms,
        bar_close_ms=value.close_time_ms,
        quote_volume=value.quote_volume,
        taker_buy_quote_volume=value.taker_buy_quote_volume,
        signed_normal_notional=signed,
        normal_notional=normal,
        total_trade_notional=total,
        signed_share=share,
        proxy_assumption="ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY",
        _factory_token=_ENTRY_FACTORY_TOKEN,
    )


def _bar_value_from_economic(
    value: ParticipationHistoricalKlineEconomicEntryV2,
) -> ParticipationFlowBarValueV2:
    _validate_economic_entry(value)
    try:
        return build_participation_flow_bar_value_v2(
            bar_open_ms=value.bar_open_ms,
            bar_close_ms=value.bar_close_ms,
            signed_normal_notional=value.signed_normal_notional,
            normal_notional=value.normal_notional,
            total_trade_notional=value.total_trade_notional,
            signed_share=value.signed_share,
        )
    except ParticipationFlowContractErrorV2 as exc:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy cannot mint a shared flow bar"
        ) from exc


def _validate_source_entry(value: ParticipationHistoricalKlineSourceEntryV2) -> None:
    if value.market is not Market.FUTURES or value.interval != "5m" or value.is_closed is not True:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline source entry identity is not exact"
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
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical source prices must be positive"
        )
    if (
        min(
            value.volume,
            value.quote_volume,
            value.taker_buy_base_volume,
            value.taker_buy_quote_volume,
        )
        < 0
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical source volumes must be nonnegative"
        )
    if (
        value.high < max(value.open, value.close)
        or value.low > min(value.open, value.close)
        or value.low > value.high
        or value.taker_buy_base_volume > value.volume
        or value.taker_buy_quote_volume > value.quote_volume
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical source kline fields are inconsistent"
        )
    if type(value.trade_count) is not int or not 0 <= value.trade_count <= _MAX_CANONICAL_INTEGER:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical source trade_count is outside canonical bounds"
        )


def _validate_economic_entry(
    value: ParticipationHistoricalKlineEconomicEntryV2,
) -> None:
    _validate_slot(value.bar_open_ms, value.bar_close_ms)
    if value.proxy_assumption != PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical economic entry must seal the all-normal proxy assumption"
        )
    for number, name in (
        (value.quote_volume, "quote_volume"),
        (value.taker_buy_quote_volume, "taker_buy_quote_volume"),
        (value.signed_normal_notional, "signed_normal_notional"),
        (value.normal_notional, "normal_notional"),
        (value.total_trade_notional, "total_trade_notional"),
    ):
        _validate_decimal(number, name)
    if value.quote_volume < 0 or not 0 <= value.taker_buy_quote_volume <= value.quote_volume:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical economic quote volumes are inconsistent"
        )
    try:
        with localcontext(protocol_decimal_context_v2()):
            expected_signed = Decimal(2) * value.taker_buy_quote_volume - value.quote_volume
            expected_share = (
                expected_signed / value.quote_volume if value.quote_volume > 0 else None
            )
    except DecimalException as exc:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical economic Decimal arithmetic is outside protocol bounds"
        ) from exc
    if (
        value.normal_notional != value.quote_volume
        or value.total_trade_notional != value.quote_volume
        or value.signed_normal_notional != expected_signed
        or value.signed_share != expected_share
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical economic values contradict the frozen kline proxy formula"
        )


def _validate_projection(value: ParticipationHistoricalKlineProxyV2) -> None:
    if value.schema_version != _SCHEMA_VERSION:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "unsupported historical kline proxy schema"
        )
    _validate_identity(value.attempt_id, "attempt_id")
    _validate_sha256(value.dataset_sha256, "dataset_sha256")
    _validate_symbol(value.symbol)
    _validate_decision_slot(value.bar_open_ms)
    expected_close = value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    expected_first = value.bar_open_ms - ROBUST_Z_PRIOR_WINDOW_V2 * FIVE_MINUTE_MS_V2
    if (
        value.market is not Market.FUTURES
        or value.interval != "5m"
        or value.bar_close_ms != expected_close
        or value.expected_first_slot_open_ms != expected_first
        or value.historical_slice_through_ms != expected_close
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline projection identity or range differs"
        )
    if value.rule_version != PARTICIPATION_FLOW_RULE_VERSION_V2:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline projection rule version differs"
        )
    if (
        value.proxy_assumption != PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2
        or value.authority_status != PARTICIPATION_HISTORICAL_KLINE_PROXY_AUTHORITY_STATUS_V2
        or value.slot_absence_interpretation != "UNKNOWN_NOT_ZERO"
        or value.expected_slot_count != PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2
        or value.historical_diagnostic_only is not True
        or value.outcome_data_read is not False
        or value.exact_agg_trade_m1_equivalent is not False
        or value.verified_raw_membership_m0_bound is not False
        or value.strict_source_parser_m1_bound is not False
        or value.causal_cursor_finality_m2_bound is not False
        or value.causal_inputs_complete is not False
        or value.producer_ready is not False
        or value.promoting_eligible is not False
        or value.probability_eligible is not False
        or value.data_through_ms is not None
        or value.m0_root_sha256 is not None
        or value.m1_payload_sha256 is not None
        or value.m2_certificate_sha256 is not None
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical proxy authority must remain explicitly non-promoting"
        )
    if (
        type(value.ordered_source_rows) is not tuple
        or not value.ordered_source_rows
        or len(value.ordered_source_rows)
        > PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2
        or type(value.economic_flow_rows) is not tuple
        or len(value.economic_flow_rows) != len(value.ordered_source_rows)
        or type(value.observed_slot_values) is not tuple
        or len(value.observed_slot_values) != len(value.ordered_source_rows)
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical proxy row collections are inconsistent"
        )
    previous_open: int | None = None
    for row in value.ordered_source_rows:
        if not isinstance(row, ParticipationHistoricalKlineSourceEntryV2):
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical proxy source collection contains an invalid entry"
            )
        _validate_source_entry(row)
        if (
            row.symbol != value.symbol
            or not value.expected_first_slot_open_ms <= row.open_time_ms <= value.bar_open_ms
            or (previous_open is not None and row.open_time_ms <= previous_open)
        ):
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical proxy source order, identity, or range differs"
            )
        previous_open = row.open_time_ms
    for row in value.economic_flow_rows:
        if not isinstance(row, ParticipationHistoricalKlineEconomicEntryV2):
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical proxy economic collection contains an invalid entry"
            )
        _validate_economic_entry(row)
    for row in value.observed_slot_values:
        try:
            canonical_participation_flow_bar_value_v2(row)
        except ParticipationFlowContractErrorV2 as exc:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical proxy contains a noncanonical shared bar value"
            ) from exc
    _validate_slot_tuple(
        value.missing_slot_open_ms,
        "missing_slot_open_ms",
        lower=value.expected_first_slot_open_ms,
        upper=value.bar_open_ms,
    )
    _validate_slot_tuple(
        value.internal_gap_after_open_ms,
        "internal_gap_after_open_ms",
        lower=value.expected_first_slot_open_ms,
        upper=value.bar_open_ms - FIVE_MINUTE_MS_V2,
    )
    _validate_sha256(value.source_lineage_root_sha256, "source_lineage_root_sha256")
    _validate_sha256(value.economic_flow_root_sha256, "economic_flow_root_sha256")
    if value.calculation is not None:
        try:
            canonical_participation_flow_calculation_v2(value.calculation)
        except ParticipationFlowContractErrorV2 as exc:
            raise ParticipationHistoricalKlineProxyContractErrorV2(
                "historical proxy contains a noncanonical shared calculation"
            ) from exc
    if not isinstance(value.status, ParticipationHistoricalKlineProxyStatusV2):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical proxy status enum differs"
        )
    _validate_reasons(value.reasons)


def _projection_status(
    *,
    missing_slots: tuple[int, ...],
    internal_gaps: tuple[int, ...],
    calculation: ParticipationFlowCalculationV2 | None,
) -> ParticipationHistoricalKlineProxyStatusV2:
    if internal_gaps:
        return ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_INTERNAL_GAP_UNKNOWN
    if missing_slots:
        return ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN
    if calculation is not None and calculation.ready:
        return ParticipationHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY
    return ParticipationHistoricalKlineProxyStatusV2.NUMERIC_NONREADY_PROXY


def _projection_reasons(
    status: ParticipationHistoricalKlineProxyStatusV2,
    calculation: ParticipationFlowCalculationV2 | None,
) -> tuple[str, ...]:
    status_reason = {
        ParticipationHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY: (
            calculation.reason if calculation is not None else "NUMERIC_CALCULATION_ABSENT"
        ),
        ParticipationHistoricalKlineProxyStatusV2.NUMERIC_NONREADY_PROXY: (
            calculation.reason if calculation is not None else "NUMERIC_CALCULATION_ABSENT"
        ),
        ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN: (
            "EXPECTED_KLINE_SLOT_MISSING_UNKNOWN_NOT_ZERO"
        ),
        ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_INTERNAL_GAP_UNKNOWN: (
            "HISTORICAL_KLINE_INTERNAL_GAP_UNKNOWN_NOT_ZERO"
        ),
    }[status]
    return (
        "HISTORICAL_KLINE_PROXY_DIAGNOSTIC_ONLY",
        PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2,
        "QUOTE_VOLUME_AND_TAKER_BUY_QUOTE_VOLUME_ONLY",
        "OUTCOME_LABEL_FILL_AND_RETURN_DATA_NOT_READ",
        "NOT_EXACT_AGGTRADE_M1_EQUIVALENT",
        status_reason,
        "M0_M1_M2_PRODUCER_PROMOTION_AND_PROBABILITY_AUTHORITY_ABSENT",
    )


def _source_root(
    rows: tuple[ParticipationHistoricalKlineSourceEntryV2, ...],
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
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "dataset_sha256": dataset_sha256,
                "expected_first_slot_open_ms": expected_first_slot_open_ms,
                "interval": "5m",
                "market": Market.FUTURES.value,
                "ordered_source_rows": [_source_document(row) for row in rows],
                "outcome_data_read": False,
                "proxy_assumption": PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2,
                "schema_version": "r4b_participation_historical_kline_source_root_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _economic_root(
    rows: tuple[ParticipationHistoricalKlineEconomicEntryV2, ...],
    values: tuple[ParticipationFlowBarValueV2, ...],
    *,
    symbol: str,
    bar_open_ms: int,
    bar_close_ms: int,
    expected_first_slot_open_ms: int,
) -> str:
    return hashlib.sha256(
        _ECONOMIC_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "expected_first_slot_open_ms": expected_first_slot_open_ms,
                "interval": "5m",
                "market": Market.FUTURES.value,
                "observed_bar_value_sha256": [row.bar_value_sha256 for row in values],
                "ordered_economic_rows": [_economic_document(row) for row in rows],
                "proxy_assumption": PARTICIPATION_HISTORICAL_KLINE_PROXY_ASSUMPTION_V2,
                "schema_version": "r4b_participation_historical_kline_economic_root_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _projection_document(
    value: ParticipationHistoricalKlineProxyV2,
    *,
    include_projection_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "authority_status": value.authority_status,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "calculation": _calculation_summary(value.calculation),
        "causal_cursor_finality_m2_bound": value.causal_cursor_finality_m2_bound,
        "causal_inputs_complete": value.causal_inputs_complete,
        "data_through_ms": value.data_through_ms,
        "dataset_sha256": value.dataset_sha256,
        "economic_flow_root_sha256": value.economic_flow_root_sha256,
        "economic_flow_rows": [_economic_document(row) for row in value.economic_flow_rows],
        "exact_agg_trade_m1_equivalent": value.exact_agg_trade_m1_equivalent,
        "expected_first_slot_open_ms": value.expected_first_slot_open_ms,
        "expected_slot_count": value.expected_slot_count,
        "historical_diagnostic_only": value.historical_diagnostic_only,
        "historical_slice_through_ms": value.historical_slice_through_ms,
        "internal_gap_after_open_ms": list(value.internal_gap_after_open_ms),
        "interval": value.interval,
        "m0_root_sha256": value.m0_root_sha256,
        "m1_payload_sha256": value.m1_payload_sha256,
        "m2_certificate_sha256": value.m2_certificate_sha256,
        "market": value.market.value,
        "missing_slot_open_ms": list(value.missing_slot_open_ms),
        "observed_slot_values": [_bar_value_summary(row) for row in value.observed_slot_values],
        "ordered_source_rows": [_source_document(row) for row in value.ordered_source_rows],
        "outcome_data_read": value.outcome_data_read,
        "probability_eligible": value.probability_eligible,
        "producer_ready": value.producer_ready,
        "promoting_eligible": value.promoting_eligible,
        "proxy_assumption": value.proxy_assumption,
        "reasons": list(value.reasons),
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "slot_absence_interpretation": value.slot_absence_interpretation,
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "status": value.status.value,
        "strict_source_parser_m1_bound": value.strict_source_parser_m1_bound,
        "symbol": value.symbol,
        "verified_raw_membership_m0_bound": value.verified_raw_membership_m0_bound,
        "attempt_id": value.attempt_id,
    }
    if include_projection_hash:
        document["projection_sha256"] = value.projection_sha256
    return document


def _source_document(value: ParticipationHistoricalKlineSourceEntryV2) -> dict[str, object]:
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
        "schema_version": _SOURCE_ENTRY_SCHEMA,
        "symbol": value.symbol,
        "taker_buy_base_volume": str(value.taker_buy_base_volume),
        "taker_buy_quote_volume": str(value.taker_buy_quote_volume),
        "trade_count": value.trade_count,
        "volume": str(value.volume),
    }


def _economic_document(
    value: ParticipationHistoricalKlineEconomicEntryV2,
) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "normal_notional": str(value.normal_notional),
        "proxy_assumption": value.proxy_assumption,
        "quote_volume": str(value.quote_volume),
        "schema_version": _ECONOMIC_ENTRY_SCHEMA,
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": _decimal_or_none(value.signed_share),
        "taker_buy_quote_volume": str(value.taker_buy_quote_volume),
        "total_trade_notional": str(value.total_trade_notional),
    }


def _bar_value_summary(value: ParticipationFlowBarValueV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "bar_value_sha256": value.bar_value_sha256,
        "normal_notional": str(value.normal_notional),
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": _decimal_or_none(value.signed_share),
        "total_trade_notional": str(value.total_trade_notional),
    }


def _calculation_summary(
    value: ParticipationFlowCalculationV2 | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "activity_support": _decimal_or_none(value.activity_support),
        "calculation_sha256": value.calculation_sha256,
        "current_signed_share": _decimal_or_none(value.current_signed_share),
        "current_total_trade_notional": _decimal_or_none(value.current_total_trade_notional),
        "direction": value.direction,
        "prior_observation_count": value.prior_observation_count,
        "prior_signed_share_location": _decimal_or_none(value.prior_signed_share_location),
        "prior_signed_share_mad": _decimal_or_none(value.prior_signed_share_mad),
        "prior_signed_share_scale": _decimal_or_none(value.prior_signed_share_scale),
        "prior_total_notional_median": _decimal_or_none(value.prior_total_notional_median),
        "reason": value.reason,
        "rule_version": value.rule_version,
        "scaled_signed_share_u": _decimal_or_none(value.scaled_signed_share_u),
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }


def _expected_slots(first_open_ms: int) -> tuple[int, ...]:
    return tuple(
        first_open_ms + index * FIVE_MINUTE_MS_V2
        for index in range(PARTICIPATION_HISTORICAL_KLINE_PROXY_EXPECTED_SLOT_COUNT_V2)
    )


def _internal_gap_after(
    rows: tuple[ParticipationHistoricalKlineSourceEntryV2, ...],
) -> tuple[int, ...]:
    return tuple(
        previous.open_time_ms
        for previous, current in pairwise(rows)
        if current.open_time_ms != previous.open_time_ms + FIVE_MINUTE_MS_V2
    )


def _validate_slot_tuple(
    values: tuple[int, ...],
    name: str,
    *,
    lower: int,
    upper: int,
) -> None:
    if (
        type(values) is not tuple
        or tuple(sorted(values)) != values
        or len(set(values)) != len(values)
        or any(
            type(item) is not int or item < lower or item > upper or item % FIVE_MINUTE_MS_V2 != 0
            for item in values
        )
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            f"{name} must contain unique ordered in-range 5m slots"
        )


def _validate_decision_slot(value: int) -> None:
    if (
        type(value) is not int
        or value < 0
        or value > _MAX_CANONICAL_INTEGER - FIVE_MINUTE_MS_V2
        or value % FIVE_MINUTE_MS_V2 != 0
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical proxy decision bar must be an aligned canonical 5m slot"
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
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline must bind one exact aligned 5m slot"
        )


def _validate_decimal(value: Decimal, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ParticipationHistoricalKlineProxyContractErrorV2(f"{name} must be a finite Decimal")


def _validate_identity(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            f"{name} must be nonempty canonical text"
        )


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline symbol must be canonical uppercase USDT"
        )


def _validate_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if (
        type(values) is not tuple
        or not values
        or len(set(values)) != len(values)
        or any(
            not isinstance(value, str) or _REASON_RE.fullmatch(value) is None for value in values
        )
    ):
        raise ParticipationHistoricalKlineProxyContractErrorV2(
            "historical kline proxy reasons must be unique canonical codes"
        )


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
