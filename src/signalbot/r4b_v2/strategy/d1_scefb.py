"""Outcome-blind, disconnected D1 SCEFB-5M rule.

The module implements only the frozen arithmetic in
``r4b-v2-d1-scefb-5m-preregistration-v0.md``.  It has no I/O, mutable state,
outcome access, PAPER fill authority, or order-placement capability.  Inputs
and outputs are factory-sealed so callers cannot instantiate an apparently
authoritative decision by filling a dataclass directly.

The five-minute history contract is unambiguous here: ``prior_bars`` contains
289 contiguous bars before ``current_bar``.  Element zero supplies one anchor
close and elements 1..288 are exactly the 288 ATR/participation calculation
bars.  ``current_bar`` is signal bar ``t`` and is never included in a prior
window.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)

D1_SCEFB_RULE_VERSION_V0: Final = "D1_SCEFB_5M_PREREG_V0"
D1_AUTHORITY_BINDING_V0: Final = "M0_M1_M2_UNBOUND"
D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0: Final = 289
D1_CALCULATION_BAR_COUNT_V0: Final = 288
D1_HOURLY_BAR_COUNT_V0: Final = 250
D1_CHANNEL_BAR_COUNT_V0: Final = 24
D1_ATR_PERIOD_V0: Final = 14
D1_HARD_HORIZON_BARS_V0: Final = 24

_HOUR_MS: Final = 3_600_000
_MAD_SCALE: Final = Decimal("1.4826")
_COMPRESSION_MAX: Final = Decimal("0.70")
_EXPANSION_MIN: Final = Decimal("1.50")
_EXPANSION_MAX: Final = Decimal("3.00")
_ATR_FRACTION_MIN: Final = Decimal("0.0035")
_BREAKOUT_MIN_ATR: Final = Decimal("0.10")
_BREAKOUT_MAX_ATR: Final = Decimal("0.50")
_CLOSE_LOCATION_MIN: Final = Decimal("0.75")
_PRIOR_QUOTE_VOLUME_MIN: Final = Decimal("100000")
_TAKER_IMBALANCE_MIN: Final = Decimal("0.20")
_ROBUST_Z_MIN: Final = Decimal("2.00")
_ACTIVITY_MIN: Final = Decimal("2.00")
_ADVERSE_ATR: Final = Decimal("0.80")
_PROFIT_ATR: Final = Decimal("3.00")

_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")

_FIVE_MINUTE_BAR_FACTORY_TOKEN: Final = object()
_HOURLY_BAR_FACTORY_TOKEN: Final = object()
_ENTRY_INPUT_FACTORY_TOKEN: Final = object()
_ENTRY_DECISION_FACTORY_TOKEN: Final = object()
_POSITION_FACTORY_TOKEN: Final = object()
_EXIT_INPUT_FACTORY_TOKEN: Final = object()
_EXIT_DECISION_FACTORY_TOKEN: Final = object()

_FIVE_MINUTE_BAR_HASH_DOMAIN: Final = b"D1_SCEFB_5M_BAR_V0\0"
_HOURLY_BAR_HASH_DOMAIN: Final = b"D1_SCEFB_1H_BAR_V0\0"
_ENTRY_INPUT_HASH_DOMAIN: Final = b"D1_SCEFB_ENTRY_INPUT_V0\0"
_ENTRY_EVENT_ID_DOMAIN: Final = b"D1_SCEFB_ENTRY_EVENT_ID_V0\0"
_ENTRY_PAYLOAD_HASH_DOMAIN: Final = b"D1_SCEFB_ENTRY_PAYLOAD_V0\0"
_POSITION_HASH_DOMAIN: Final = b"D1_SCEFB_POSITION_ANCHOR_V0\0"
_EXIT_INPUT_HASH_DOMAIN: Final = b"D1_SCEFB_EXIT_INPUT_V0\0"
_EXIT_EVENT_ID_DOMAIN: Final = b"D1_SCEFB_EXIT_EVENT_ID_V0\0"
_EXIT_PAYLOAD_HASH_DOMAIN: Final = b"D1_SCEFB_EXIT_PAYLOAD_V0\0"


class D1ScefbContractErrorV0(ValueError):
    """Raised when a D1 value violates the disconnected rule contract."""


class D1SideV0(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class D1EntryStatusV0(StrEnum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    INCONCLUSIVE = "INCONCLUSIVE"


class D1ExitStatusV0(StrEnum):
    KEEP = "KEEP"
    EXIT = "EXIT"
    INCONCLUSIVE_EXIT = "INCONCLUSIVE_EXIT"


class D1ExitActionV0(StrEnum):
    KEEP = "KEEP"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"


class D1ExitReasonV0(StrEnum):
    KEEP = "KEEP"
    AUTHORITY_LOSS = "AUTHORITY_LOSS"
    ADVERSE_CLOSE = "ADVERSE_CLOSE"
    STRUCTURE_FAILURE = "STRUCTURE_FAILURE"
    PROFIT_CLOSE = "PROFIT_CLOSE"
    HARD_HORIZON = "HARD_HORIZON"


class D1EntryReferenceKindV0(StrEnum):
    HISTORICAL_OPEN_PROXY = "HISTORICAL_OPEN_PROXY"
    UNBOUND_PAPER_VWAP_REFERENCE = "UNBOUND_PAPER_VWAP_REFERENCE"


@dataclass(frozen=True, slots=True)
class D1FiveMinuteBarV0:
    """One immutable five-minute observation; readiness is evaluated later."""

    open_ms: int
    close_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal
    data_through_ms: int
    receipt_ms: int
    is_closed: bool
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default="d1_scefb_five_minute_bar_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FIVE_MINUTE_BAR_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 five-minute bars must be factory-created")
        _validate_bar_slot(self.open_ms, self.close_ms, FIVE_MINUTE_MS_V2, "5m")
        _validate_ohlc(
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
        )
        _validate_nonnegative_decimal(self.quote_volume, "quote_volume")
        _validate_nonnegative_decimal(
            self.taker_buy_quote_volume,
            "taker_buy_quote_volume",
        )
        if self.taker_buy_quote_volume > self.quote_volume:
            raise D1ScefbContractErrorV0("taker_buy_quote_volume cannot exceed quote_volume")
        _validate_nonnegative_int(self.data_through_ms, "data_through_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if type(self.is_closed) is not bool:
            raise D1ScefbContractErrorV0("is_closed must be a boolean")
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _FIVE_MINUTE_BAR_HASH_DOMAIN,
                _five_minute_bar_document(self, include_payload_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D1HourlyBarV0:
    """One immutable one-hour close observation; readiness is evaluated later."""

    open_ms: int
    close_ms: int
    close_price: Decimal
    data_through_ms: int
    receipt_ms: int
    is_closed: bool
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default="d1_scefb_hourly_bar_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _HOURLY_BAR_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 hourly bars must be factory-created")
        _validate_bar_slot(self.open_ms, self.close_ms, _HOUR_MS, "1h")
        _validate_positive_decimal(self.close_price, "close_price")
        _validate_nonnegative_int(self.data_through_ms, "data_through_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if type(self.is_closed) is not bool:
            raise D1ScefbContractErrorV0("is_closed must be a boolean")
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _HOURLY_BAR_HASH_DOMAIN,
                _hourly_bar_document(self, include_payload_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D1EntryInputV0:
    """Canonical disconnected input for one signal-bar evaluation."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    source_root_sha256: str
    prior_bars: tuple[D1FiveMinuteBarV0, ...]
    current_bar: D1FiveMinuteBarV0
    hourly_bars: tuple[D1HourlyBarV0, ...]
    required_fields_complete: bool
    _factory_token: InitVar[object | None] = None
    decision_cutoff_ms: int = field(init=False)
    input_sha256: str = field(init=False)
    authority_binding: str = field(init=False, default=D1_AUTHORITY_BINDING_V0)
    causal_authority_bound: bool = field(init=False, default=False)
    paper_input_authorized: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_scefb_entry_input_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_INPUT_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 entry inputs must be factory-created")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise D1ScefbContractErrorV0("D1 accepts USD-M Futures only")
        _validate_sha256(self.source_root_sha256, "source_root_sha256")
        _require_exact_tuple(self.prior_bars, D1FiveMinuteBarV0, "prior_bars")
        if type(self.current_bar) is not D1FiveMinuteBarV0:
            raise D1ScefbContractErrorV0("current_bar has the wrong sealed type")
        _require_exact_tuple(self.hourly_bars, D1HourlyBarV0, "hourly_bars")
        if type(self.required_fields_complete) is not bool:
            raise D1ScefbContractErrorV0("required_fields_complete must be a boolean")
        decision_cutoff_ms = self.current_bar.close_ms + DECISION_DELAY_MS_V2
        object.__setattr__(self, "decision_cutoff_ms", decision_cutoff_ms)
        object.__setattr__(
            self,
            "input_sha256",
            _hash_document(
                _ENTRY_INPUT_HASH_DOMAIN,
                _entry_input_document(self, include_input_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D1EntryDecisionV0:
    """One deterministic alert-only D1 conjunction decision."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    source_root_sha256: str
    input_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    status: D1EntryStatusV0
    side: D1SideV0 | None
    reasons: tuple[str, ...]
    invalidation: str
    signal_close: Decimal | None
    frozen_atr: Decimal | None
    frozen_channel_upper: Decimal | None
    frozen_channel_lower: Decimal | None
    compression: Decimal | None
    current_expansion: Decimal | None
    atr_fraction: Decimal | None
    prior_median_quote_volume: Decimal | None
    current_taker_imbalance: Decimal | None
    taker_robust_z: Decimal | None
    activity: Decimal | None
    latest_hour_close: Decimal | None
    ema20_hourly: Decimal | None
    ema50_hourly: Decimal | None
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=D1_SCEFB_RULE_VERSION_V0)
    authority_binding: str = field(init=False, default=D1_AUTHORITY_BINDING_V0)
    causal_authority_bound: bool = field(init=False, default=False)
    paper_input_authorized: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    paper_entry_evaluated: bool = field(init=False, default=False)
    passed_gate_count_is_probability: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_scefb_entry_decision_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ENTRY_DECISION_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 entry decisions must be evaluator-created")
        _validate_common_identity(
            self.attempt_id,
            self.symbol,
            self.venue,
            self.source_root_sha256,
            self.input_sha256,
        )
        _validate_decision_clock(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_reasons(self.reasons)
        _validate_identity(self.invalidation, "invalidation")
        _validate_entry_decision_state(self)
        object.__setattr__(
            self,
            "event_id",
            _hash_document(_ENTRY_EVENT_ID_DOMAIN, _entry_identity_document(self)),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _ENTRY_PAYLOAD_HASH_DOMAIN,
                _entry_decision_document(self, include_payload_hash=False),
            ),
        )

    @property
    def alert_intent(self) -> bool:
        return self.status is D1EntryStatusV0.SIGNAL


@dataclass(frozen=True, slots=True)
class D1PaperPositionAnchorV0:
    """Unbound PAPER-exit anchor derived from a D1 signal and supplied VWAP.

    The type freezes strategy thresholds but deliberately does not assert that
    a fill occurred.  A future runtime owner must bind an authoritative PAPER
    admission receipt before this can affect a prospective result.
    ``entry_bar_*`` preserves the signal-bar identity; ``entry_fill_ms`` owns
    the supplied PAPER fill time and determines the containing first exit bar.
    """

    attempt_id: str
    symbol: str
    venue: VenueV2
    entry_event_id: str
    entry_bar_open_ms: int
    entry_bar_close_ms: int
    entry_fill_ms: int
    entry_reference_kind: D1EntryReferenceKindV0
    side: D1SideV0
    entry_vwap: Decimal
    signal_close: Decimal
    frozen_atr: Decimal
    frozen_channel_upper: Decimal
    frozen_channel_lower: Decimal
    entry_vwap_source_sha256: str
    _factory_token: InitVar[object | None] = None
    first_exit_bar_open_ms: int = field(init=False)
    position_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=D1_SCEFB_RULE_VERSION_V0)
    authority_binding: str = field(init=False, default=D1_AUTHORITY_BINDING_V0)
    paper_fill_claim: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_scefb_paper_position_anchor_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 PAPER position anchors must be factory-created")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise D1ScefbContractErrorV0("D1 position anchor requires USD-M Futures")
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_sha256(self.entry_vwap_source_sha256, "entry_vwap_source_sha256")
        _validate_bar_slot(
            self.entry_bar_open_ms,
            self.entry_bar_close_ms,
            FIVE_MINUTE_MS_V2,
            "entry 5m",
        )
        _validate_nonnegative_int(self.entry_fill_ms, "entry_fill_ms")
        if self.entry_fill_ms <= self.entry_bar_close_ms + DECISION_DELAY_MS_V2:
            raise D1ScefbContractErrorV0(
                "entry fill must be strictly after the signal decision cutoff"
            )
        if not isinstance(self.entry_reference_kind, D1EntryReferenceKindV0):
            raise D1ScefbContractErrorV0("entry reference kind is unsupported")
        if not isinstance(self.side, D1SideV0):
            raise D1ScefbContractErrorV0("position side must be LONG or SHORT")
        for value, field_name in (
            (self.entry_vwap, "entry_vwap"),
            (self.signal_close, "signal_close"),
            (self.frozen_atr, "frozen_atr"),
            (self.frozen_channel_upper, "frozen_channel_upper"),
            (self.frozen_channel_lower, "frozen_channel_lower"),
        ):
            _validate_positive_decimal(value, field_name)
        if self.frozen_channel_upper < self.frozen_channel_lower:
            raise D1ScefbContractErrorV0("frozen channel bounds are inverted")
        with localcontext(protocol_decimal_context_v2()):
            if abs(self.entry_vwap - self.signal_close) > _BREAKOUT_MAX_ATR * self.frozen_atr:
                raise D1ScefbContractErrorV0(
                    "entry VWAP exceeds the frozen 0.50 ATR admission distance"
                )
        object.__setattr__(
            self,
            "first_exit_bar_open_ms",
            (self.entry_fill_ms // FIVE_MINUTE_MS_V2) * FIVE_MINUTE_MS_V2,
        )
        object.__setattr__(
            self,
            "position_sha256",
            _hash_document(_POSITION_HASH_DOMAIN, _position_document(self)),
        )


@dataclass(frozen=True, slots=True)
class D1ExitInputV0:
    """Canonical bounded path for one post-entry closed-bar exit decision."""

    position: D1PaperPositionAnchorV0
    source_root_sha256: str
    bars_since_entry: tuple[D1FiveMinuteBarV0, ...]
    required_data_available: bool
    authority_continuity_declared: bool
    _factory_token: InitVar[object | None] = None
    decision_cutoff_ms: int = field(init=False)
    input_sha256: str = field(init=False)
    authority_binding: str = field(init=False, default=D1_AUTHORITY_BINDING_V0)
    causal_authority_bound: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_scefb_exit_input_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _EXIT_INPUT_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 exit inputs must be factory-created")
        if type(self.position) is not D1PaperPositionAnchorV0:
            raise D1ScefbContractErrorV0("position has the wrong sealed type")
        _validate_sha256(self.source_root_sha256, "source_root_sha256")
        _require_exact_tuple(
            self.bars_since_entry,
            D1FiveMinuteBarV0,
            "bars_since_entry",
        )
        if not self.bars_since_entry:
            raise D1ScefbContractErrorV0("bars_since_entry must contain the current bar")
        for value, field_name in (
            (self.required_data_available, "required_data_available"),
            (self.authority_continuity_declared, "authority_continuity_declared"),
        ):
            if type(value) is not bool:
                raise D1ScefbContractErrorV0(f"{field_name} must be a boolean")
        object.__setattr__(
            self,
            "decision_cutoff_ms",
            self.bars_since_entry[-1].close_ms + DECISION_DELAY_MS_V2,
        )
        object.__setattr__(
            self,
            "input_sha256",
            _hash_document(
                _EXIT_INPUT_HASH_DOMAIN,
                _exit_input_document(self, include_input_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D1ExitDecisionV0:
    """Deterministic alert/PAPER intent; never a fill or production order."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    entry_event_id: str
    position_sha256: str
    source_root_sha256: str
    input_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    bars_elapsed: int
    close_price: Decimal
    status: D1ExitStatusV0
    action: D1ExitActionV0
    exit_reason: D1ExitReasonV0
    reasons: tuple[str, ...]
    invalidation: str
    adverse_boundary: Decimal
    structure_boundary: Decimal
    profit_boundary: Decimal
    interval_conclusive: bool
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=D1_SCEFB_RULE_VERSION_V0)
    authority_binding: str = field(init=False, default=D1_AUTHORITY_BINDING_V0)
    causal_authority_bound: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    paper_exit_fill_claim: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default="d1_scefb_exit_decision_v0")

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _EXIT_DECISION_FACTORY_TOKEN:
            raise D1ScefbContractErrorV0("D1 exit decisions must be evaluator-created")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise D1ScefbContractErrorV0("D1 exit decision requires USD-M Futures")
        for value, field_name in (
            (self.entry_event_id, "entry_event_id"),
            (self.position_sha256, "position_sha256"),
            (self.source_root_sha256, "source_root_sha256"),
            (self.input_sha256, "input_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_decision_clock(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if type(self.bars_elapsed) is not int or self.bars_elapsed < 1:
            raise D1ScefbContractErrorV0("bars_elapsed must be a positive integer")
        for value, field_name in (
            (self.close_price, "close_price"),
            (self.adverse_boundary, "adverse_boundary"),
            (self.structure_boundary, "structure_boundary"),
            (self.profit_boundary, "profit_boundary"),
        ):
            _validate_finite_decimal(value, field_name)
        _validate_reasons(self.reasons)
        _validate_identity(self.invalidation, "invalidation")
        _validate_exit_decision_state(self)
        object.__setattr__(
            self,
            "event_id",
            _hash_document(_EXIT_EVENT_ID_DOMAIN, _exit_identity_document(self)),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _EXIT_PAYLOAD_HASH_DOMAIN,
                _exit_decision_document(self, include_payload_hash=False),
            ),
        )

    @property
    def alert_intent(self) -> bool:
        return self.action is not D1ExitActionV0.KEEP

    @property
    def paper_exit_intent(self) -> bool:
        return self.action is not D1ExitActionV0.KEEP


@dataclass(frozen=True, slots=True)
class _D1EntryMetricsV0:
    signal_close: Decimal
    frozen_atr: Decimal
    frozen_channel_upper: Decimal
    frozen_channel_lower: Decimal
    compression: Decimal
    current_expansion: Decimal
    atr_fraction: Decimal
    prior_median_quote_volume: Decimal
    current_taker_imbalance: Decimal
    taker_robust_z: Decimal
    activity: Decimal
    latest_hour_close: Decimal
    ema20_hourly: Decimal
    ema50_hourly: Decimal
    previous_close: Decimal
    long_breakout_atr: Decimal
    short_breakout_atr: Decimal
    long_close_location: Decimal
    short_close_location: Decimal


def build_d1_five_minute_bar_v0(
    *,
    open_ms: int,
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    quote_volume: Decimal,
    taker_buy_quote_volume: Decimal,
    data_through_ms: int | None = None,
    receipt_ms: int | None = None,
    is_closed: bool = True,
) -> D1FiveMinuteBarV0:
    """Build one sealed five-minute observation without evaluating readiness."""

    _validate_nonnegative_int(open_ms, "open_ms")
    close_ms = open_ms + FIVE_MINUTE_MS_V2 - 1
    return D1FiveMinuteBarV0(
        open_ms=open_ms,
        close_ms=close_ms,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        quote_volume=quote_volume,
        taker_buy_quote_volume=taker_buy_quote_volume,
        data_through_ms=close_ms if data_through_ms is None else data_through_ms,
        receipt_ms=close_ms if receipt_ms is None else receipt_ms,
        is_closed=is_closed,
        _factory_token=_FIVE_MINUTE_BAR_FACTORY_TOKEN,
    )


def build_d1_hourly_bar_v0(
    *,
    open_ms: int,
    close_price: Decimal,
    data_through_ms: int | None = None,
    receipt_ms: int | None = None,
    is_closed: bool = True,
) -> D1HourlyBarV0:
    """Build one sealed one-hour close observation."""

    _validate_nonnegative_int(open_ms, "open_ms")
    close_ms = open_ms + _HOUR_MS - 1
    return D1HourlyBarV0(
        open_ms=open_ms,
        close_ms=close_ms,
        close_price=close_price,
        data_through_ms=close_ms if data_through_ms is None else data_through_ms,
        receipt_ms=close_ms if receipt_ms is None else receipt_ms,
        is_closed=is_closed,
        _factory_token=_HOURLY_BAR_FACTORY_TOKEN,
    )


def build_d1_entry_input_v0(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    source_root_sha256: str,
    prior_bars: tuple[D1FiveMinuteBarV0, ...],
    current_bar: D1FiveMinuteBarV0,
    hourly_bars: tuple[D1HourlyBarV0, ...],
    required_fields_complete: bool = True,
) -> D1EntryInputV0:
    """Seal one disconnected D1 entry input; temporal readiness stays observable."""

    return D1EntryInputV0(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        source_root_sha256=source_root_sha256,
        prior_bars=prior_bars,
        current_bar=current_bar,
        hourly_bars=hourly_bars,
        required_fields_complete=required_fields_complete,
        _factory_token=_ENTRY_INPUT_FACTORY_TOKEN,
    )


def evaluate_d1_entry_v0(item: D1EntryInputV0) -> D1EntryDecisionV0:
    """Evaluate the exact D1 long/short conjunction with protocol Decimal math."""

    if type(item) is not D1EntryInputV0:
        raise D1ScefbContractErrorV0("item must be exact D1EntryInputV0")
    canonical_d1_entry_input_v0(item)
    readiness_failures = _entry_readiness_failures(item)
    if readiness_failures:
        return _entry_decision(
            item,
            status=D1EntryStatusV0.INCONCLUSIVE,
            side=None,
            reasons=readiness_failures,
            invalidation="NO_POSITION_CREATED_INCONCLUSIVE_INPUT",
            metrics=None,
        )
    calculation_bars = item.prior_bars[1:]
    try:
        metrics = _compute_entry_metrics(item, calculation_bars)
    except _D1NonReadyArithmeticV0 as error:
        return _entry_decision(
            item,
            status=D1EntryStatusV0.INCONCLUSIVE,
            side=None,
            reasons=(error.reason,),
            invalidation="NO_POSITION_CREATED_INCONCLUSIVE_ARITHMETIC",
            metrics=None,
        )

    if metrics.signal_close > metrics.frozen_channel_upper:
        failures = _long_gate_failures(metrics)
        side = D1SideV0.LONG
    elif metrics.signal_close < metrics.frozen_channel_lower:
        failures = _short_gate_failures(metrics)
        side = D1SideV0.SHORT
    else:
        failures = ("NO_PRIOR_CHANNEL_BREAKOUT",)
        side = None
    if failures:
        return _entry_decision(
            item,
            status=D1EntryStatusV0.NO_SIGNAL,
            side=None,
            reasons=failures,
            invalidation="NO_POSITION_CREATED_FAILED_D1_CONJUNCTION",
            metrics=metrics,
        )
    assert side is not None
    return _entry_decision(
        item,
        status=D1EntryStatusV0.SIGNAL,
        side=side,
        reasons=(
            "D1_FULL_SETUP",
            "G1_PRICE_STRUCTURE_AND_STRICT_PRIOR_HOURLY_TREND_PASS",
            "G2_VOLATILITY_ELIGIBILITY_PASS",
            "G3_PARTICIPATION_FLOW_CONFIRMATION_PASS",
            f"ACTION_{side.value}_ALERT_ONLY",
        ),
        invalidation=(
            "FREEZE_ATR_AND_CHANNEL; AFTER_UNBOUND_PAPER_ENTRY_VWAP_APPLY_"
            "0.80ATR_ADVERSE_3.00ATR_PROFIT_STRUCTURE_AND_24_BAR_HORIZON"
        ),
        metrics=metrics,
    )


def build_d1_paper_position_anchor_v0(
    *,
    entry_decision: D1EntryDecisionV0,
    entry_vwap: Decimal,
    entry_fill_ms: int,
    entry_reference_kind: D1EntryReferenceKindV0,
    entry_vwap_source_sha256: str,
) -> D1PaperPositionAnchorV0:
    """Freeze exit geometry from a signal without claiming a PAPER fill."""

    if type(entry_decision) is not D1EntryDecisionV0:
        raise D1ScefbContractErrorV0("entry_decision must be exact D1EntryDecisionV0")
    canonical_d1_entry_decision_v0(entry_decision)
    if entry_decision.status is not D1EntryStatusV0.SIGNAL or entry_decision.side is None:
        raise D1ScefbContractErrorV0("position anchor requires a D1 SIGNAL")
    assert entry_decision.signal_close is not None
    assert entry_decision.frozen_atr is not None
    assert entry_decision.frozen_channel_upper is not None
    assert entry_decision.frozen_channel_lower is not None
    return D1PaperPositionAnchorV0(
        attempt_id=entry_decision.attempt_id,
        symbol=entry_decision.symbol,
        venue=entry_decision.venue,
        entry_event_id=entry_decision.event_id,
        entry_bar_open_ms=entry_decision.bar_open_ms,
        entry_bar_close_ms=entry_decision.bar_close_ms,
        entry_fill_ms=entry_fill_ms,
        entry_reference_kind=entry_reference_kind,
        side=entry_decision.side,
        entry_vwap=entry_vwap,
        signal_close=entry_decision.signal_close,
        frozen_atr=entry_decision.frozen_atr,
        frozen_channel_upper=entry_decision.frozen_channel_upper,
        frozen_channel_lower=entry_decision.frozen_channel_lower,
        entry_vwap_source_sha256=entry_vwap_source_sha256,
        _factory_token=_POSITION_FACTORY_TOKEN,
    )


def build_d1_exit_input_v0(
    *,
    position: D1PaperPositionAnchorV0,
    source_root_sha256: str,
    bars_since_entry: tuple[D1FiveMinuteBarV0, ...],
    required_data_available: bool = True,
    authority_continuity_declared: bool = True,
) -> D1ExitInputV0:
    """Seal a bounded post-entry path without upgrading caller declarations."""

    return D1ExitInputV0(
        position=position,
        source_root_sha256=source_root_sha256,
        bars_since_entry=bars_since_entry,
        required_data_available=required_data_available,
        authority_continuity_declared=authority_continuity_declared,
        _factory_token=_EXIT_INPUT_FACTORY_TOKEN,
    )


def evaluate_d1_exit_v0(item: D1ExitInputV0) -> D1ExitDecisionV0:
    """Apply frozen D1 exit priority to one subsequent closed-bar path."""

    if type(item) is not D1ExitInputV0:
        raise D1ScefbContractErrorV0("item must be exact D1ExitInputV0")
    canonical_d1_exit_input_v0(item)
    position = item.position
    current = item.bars_since_entry[-1]
    with localcontext(protocol_decimal_context_v2()):
        if position.side is D1SideV0.LONG:
            adverse = position.entry_vwap - _ADVERSE_ATR * position.frozen_atr
            structure = position.frozen_channel_upper
            profit = position.entry_vwap + _PROFIT_ATR * position.frozen_atr
        else:
            adverse = position.entry_vwap + _ADVERSE_ATR * position.frozen_atr
            structure = position.frozen_channel_lower
            profit = position.entry_vwap - _PROFIT_ATR * position.frozen_atr

    authority_failures = _exit_authority_failures(item)
    if authority_failures:
        return _exit_decision(
            item,
            status=D1ExitStatusV0.INCONCLUSIVE_EXIT,
            reason=D1ExitReasonV0.AUTHORITY_LOSS,
            reasons=("MANDATORY_EXIT_AUTHORITY_OR_DATA_LOST", *authority_failures),
            invalidation="INTERVAL_INCONCLUSIVE_CAUSAL_PUBLIC_DEPTH_PAPER_EXIT_REQUIRED",
            adverse=adverse,
            structure=structure,
            profit=profit,
            interval_conclusive=False,
        )
    close = current.close_price
    if _adverse_close(position.side, close, adverse):
        reason = D1ExitReasonV0.ADVERSE_CLOSE
    elif _structure_failure(position.side, close, structure):
        reason = D1ExitReasonV0.STRUCTURE_FAILURE
    elif _profit_close(position.side, close, profit):
        reason = D1ExitReasonV0.PROFIT_CLOSE
    elif len(item.bars_since_entry) == D1_HARD_HORIZON_BARS_V0:
        reason = D1ExitReasonV0.HARD_HORIZON
    else:
        reason = D1ExitReasonV0.KEEP

    if reason is D1ExitReasonV0.KEEP:
        return _exit_decision(
            item,
            status=D1ExitStatusV0.KEEP,
            reason=reason,
            reasons=("KEEP_POSITION_NO_FROZEN_EXIT_BOUNDARY_MET",),
            invalidation="REEVALUATE_ON_NEXT_FULLY_CLOSED_5M_BAR_WITH_SAME_FROZEN_THRESHOLDS",
            adverse=adverse,
            structure=structure,
            profit=profit,
            interval_conclusive=True,
        )
    return _exit_decision(
        item,
        status=D1ExitStatusV0.EXIT,
        reason=reason,
        reasons=(f"{reason.value}_CAUSAL_PUBLIC_DEPTH_PAPER_EXIT_INTENT",),
        invalidation=f"POSITION_TERMINATED_BY_{reason.value}_IF_PAPER_EXIT_COMPLETES",
        adverse=adverse,
        structure=structure,
        profit=profit,
        interval_conclusive=True,
    )


def canonical_d1_five_minute_bar_v0(value: D1FiveMinuteBarV0) -> bytes:
    if type(value) is not D1FiveMinuteBarV0:
        raise D1ScefbContractErrorV0("value must be exact D1FiveMinuteBarV0")
    expected = _hash_document(
        _FIVE_MINUTE_BAR_HASH_DOMAIN,
        _five_minute_bar_document(value, include_payload_hash=False),
    )
    if value.payload_sha256 != expected:
        raise D1ScefbContractErrorV0("five-minute bar hash differs from canonical payload")
    return canonical_json_line(_five_minute_bar_document(value, include_payload_hash=True))


def canonical_d1_hourly_bar_v0(value: D1HourlyBarV0) -> bytes:
    if type(value) is not D1HourlyBarV0:
        raise D1ScefbContractErrorV0("value must be exact D1HourlyBarV0")
    expected = _hash_document(
        _HOURLY_BAR_HASH_DOMAIN,
        _hourly_bar_document(value, include_payload_hash=False),
    )
    if value.payload_sha256 != expected:
        raise D1ScefbContractErrorV0("hourly bar hash differs from canonical payload")
    return canonical_json_line(_hourly_bar_document(value, include_payload_hash=True))


def canonical_d1_entry_input_v0(value: D1EntryInputV0) -> bytes:
    if type(value) is not D1EntryInputV0:
        raise D1ScefbContractErrorV0("value must be exact D1EntryInputV0")
    expected = _hash_document(
        _ENTRY_INPUT_HASH_DOMAIN,
        _entry_input_document(value, include_input_hash=False),
    )
    if value.input_sha256 != expected:
        raise D1ScefbContractErrorV0("entry input hash differs from canonical payload")
    return canonical_json_line(_entry_input_document(value, include_input_hash=True))


def canonical_d1_entry_decision_v0(value: D1EntryDecisionV0) -> bytes:
    if type(value) is not D1EntryDecisionV0:
        raise D1ScefbContractErrorV0("value must be exact D1EntryDecisionV0")
    expected = _hash_document(
        _ENTRY_PAYLOAD_HASH_DOMAIN,
        _entry_decision_document(value, include_payload_hash=False),
    )
    if value.payload_sha256 != expected:
        raise D1ScefbContractErrorV0("entry decision hash differs from canonical payload")
    return canonical_json_line(_entry_decision_document(value, include_payload_hash=True))


def canonical_d1_position_anchor_v0(value: D1PaperPositionAnchorV0) -> bytes:
    if type(value) is not D1PaperPositionAnchorV0:
        raise D1ScefbContractErrorV0("value must be exact D1PaperPositionAnchorV0")
    expected = _hash_document(_POSITION_HASH_DOMAIN, _position_document(value))
    if value.position_sha256 != expected:
        raise D1ScefbContractErrorV0("position hash differs from canonical payload")
    return canonical_json_line({**_position_document(value), "position_sha256": expected})


def canonical_d1_exit_input_v0(value: D1ExitInputV0) -> bytes:
    if type(value) is not D1ExitInputV0:
        raise D1ScefbContractErrorV0("value must be exact D1ExitInputV0")
    canonical_d1_position_anchor_v0(value.position)
    expected = _hash_document(
        _EXIT_INPUT_HASH_DOMAIN,
        _exit_input_document(value, include_input_hash=False),
    )
    if value.input_sha256 != expected:
        raise D1ScefbContractErrorV0("exit input hash differs from canonical payload")
    return canonical_json_line(_exit_input_document(value, include_input_hash=True))


def canonical_d1_exit_decision_v0(value: D1ExitDecisionV0) -> bytes:
    if type(value) is not D1ExitDecisionV0:
        raise D1ScefbContractErrorV0("value must be exact D1ExitDecisionV0")
    expected = _hash_document(
        _EXIT_PAYLOAD_HASH_DOMAIN,
        _exit_decision_document(value, include_payload_hash=False),
    )
    if value.payload_sha256 != expected:
        raise D1ScefbContractErrorV0("exit decision hash differs from canonical payload")
    return canonical_json_line(_exit_decision_document(value, include_payload_hash=True))


class _D1NonReadyArithmeticV0(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _entry_readiness_failures(item: D1EntryInputV0) -> tuple[str, ...]:
    failures: list[str] = []
    if not item.required_fields_complete:
        failures.append("REQUIRED_FIELDS_INCOMPLETE")
    if len(item.prior_bars) != D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0:
        failures.append("PRIOR_5M_COUNT_NOT_289")
    if len(item.hourly_bars) != D1_HOURLY_BAR_COUNT_V0:
        failures.append("HOURLY_COUNT_NOT_250")
    all_five_minute = (*item.prior_bars, item.current_bar)
    if any(not value.is_closed for value in all_five_minute):
        failures.append("FIVE_MINUTE_BAR_NOT_FULLY_CLOSED")
    if any(not value.is_closed for value in item.hourly_bars):
        failures.append("HOURLY_BAR_NOT_FULLY_CLOSED")
    if len(item.prior_bars) == D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0:
        expected_first = item.current_bar.open_ms - (
            D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0 * FIVE_MINUTE_MS_V2
        )
        if any(
            value.open_ms != expected_first + index * FIVE_MINUTE_MS_V2
            for index, value in enumerate(item.prior_bars)
        ):
            failures.append("FIVE_MINUTE_SEQUENCE_NOT_CONTIGUOUS")
    if len(item.hourly_bars) == D1_HOURLY_BAR_COUNT_V0:
        expected_latest_close = ((item.current_bar.close_ms + 1) // _HOUR_MS) * _HOUR_MS - 1
        expected_first = expected_latest_close - (D1_HOURLY_BAR_COUNT_V0 * _HOUR_MS) + 1
        if any(
            value.open_ms != expected_first + index * _HOUR_MS
            for index, value in enumerate(item.hourly_bars)
        ):
            failures.append("HOURLY_SEQUENCE_OR_LATEST_CUTOFF_MISMATCH")
    all_observations = (*all_five_minute, *item.hourly_bars)
    if any(value.data_through_ms > item.current_bar.close_ms for value in all_observations):
        failures.append("SOURCE_DATA_AFTER_SIGNAL_CLOSE")
    if any(value.receipt_ms > item.decision_cutoff_ms for value in all_observations):
        failures.append("SOURCE_RECEIPT_AFTER_DECISION_CUTOFF")
    return tuple(failures)


def _compute_entry_metrics(
    item: D1EntryInputV0,
    calculation_bars: tuple[D1FiveMinuteBarV0, ...],
) -> _D1EntryMetricsV0:
    if len(calculation_bars) != D1_CALCULATION_BAR_COUNT_V0:
        raise _D1NonReadyArithmeticV0("CALCULATION_BAR_COUNT_NOT_288")
    with localcontext(protocol_decimal_context_v2()):
        true_ranges = _true_ranges(item.prior_bars[0].close_price, calculation_bars)
        atr = _wilder_atr(true_ranges)
        if atr == 0:
            raise _D1NonReadyArithmeticV0("ZERO_ATR14")
        baseline_range_median = _median(true_ranges[-72:-12])
        if baseline_range_median == 0:
            raise _D1NonReadyArithmeticV0("ZERO_COMPRESSION_BASELINE_MEDIAN")
        compression = _median(true_ranges[-12:]) / baseline_range_median
        prior_quote_volumes = tuple(value.quote_volume for value in calculation_bars)
        if any(value == 0 for value in prior_quote_volumes):
            raise _D1NonReadyArithmeticV0("ZERO_PRIOR_QUOTE_VOLUME_DENOMINATOR")
        prior_quote_median = _median(prior_quote_volumes)
        if prior_quote_median == 0:
            raise _D1NonReadyArithmeticV0("ZERO_PRIOR_QUOTE_VOLUME_MEDIAN")
        current = item.current_bar
        if current.quote_volume == 0:
            raise _D1NonReadyArithmeticV0("ZERO_CURRENT_QUOTE_VOLUME")
        current_range = current.high_price - current.low_price
        if current_range == 0:
            raise _D1NonReadyArithmeticV0("ZERO_CURRENT_HIGH_LOW_RANGE")
        prior_imbalances = tuple(_taker_imbalance(value) for value in calculation_bars)
        imbalance_median = _median(prior_imbalances)
        imbalance_mad = _median(tuple(abs(value - imbalance_median) for value in prior_imbalances))
        if imbalance_mad == 0:
            raise _D1NonReadyArithmeticV0("ZERO_TAKER_IMBALANCE_MAD")
        current_imbalance = _taker_imbalance(current)
        robust_z = (current_imbalance - imbalance_median) / (_MAD_SCALE * imbalance_mad)
        previous_close = calculation_bars[-1].close_price
        if previous_close == 0:
            raise _D1NonReadyArithmeticV0("ZERO_PREVIOUS_CLOSE")
        current_true_range = max(
            current.high_price - current.low_price,
            abs(current.high_price - previous_close),
            abs(current.low_price - previous_close),
        )
        channel = calculation_bars[-D1_CHANNEL_BAR_COUNT_V0:]
        upper = max(value.high_price for value in channel)
        lower = min(value.low_price for value in channel)
        ema20 = _ema(tuple(value.close_price for value in item.hourly_bars), 20)
        ema50 = _ema(tuple(value.close_price for value in item.hourly_bars), 50)
        latest_hour_close = item.hourly_bars[-1].close_price
        return _D1EntryMetricsV0(
            signal_close=current.close_price,
            frozen_atr=atr,
            frozen_channel_upper=upper,
            frozen_channel_lower=lower,
            compression=compression,
            current_expansion=current_true_range / atr,
            atr_fraction=atr / previous_close,
            prior_median_quote_volume=prior_quote_median,
            current_taker_imbalance=current_imbalance,
            taker_robust_z=robust_z,
            activity=current.quote_volume / prior_quote_median,
            latest_hour_close=latest_hour_close,
            ema20_hourly=ema20,
            ema50_hourly=ema50,
            previous_close=previous_close,
            long_breakout_atr=(current.close_price - upper) / atr,
            short_breakout_atr=(lower - current.close_price) / atr,
            long_close_location=(current.close_price - current.low_price) / current_range,
            short_close_location=(current.high_price - current.close_price) / current_range,
        )


def _long_gate_failures(metrics: _D1EntryMetricsV0) -> tuple[str, ...]:
    failures: list[str] = []
    if metrics.previous_close > metrics.frozen_channel_upper:
        failures.append("LONG_FIRST_CROSS_FAILED")
    if metrics.long_breakout_atr < _BREAKOUT_MIN_ATR:
        failures.append("LONG_BREAKOUT_ATR_LT_0_10")
    if metrics.long_breakout_atr > _BREAKOUT_MAX_ATR:
        failures.append("LONG_BREAKOUT_ATR_GT_0_50")
    if metrics.long_close_location < _CLOSE_LOCATION_MIN:
        failures.append("LONG_CLOSE_LOCATION_LT_0_75")
    if not (metrics.latest_hour_close > metrics.ema20_hourly > metrics.ema50_hourly):
        failures.append("LONG_STRICT_PRIOR_HOURLY_TREND_FAILED")
    failures.extend(_shared_gate_failures(metrics))
    if metrics.current_taker_imbalance < _TAKER_IMBALANCE_MIN:
        failures.append("LONG_TAKER_IMBALANCE_LT_0_20")
    if metrics.taker_robust_z < _ROBUST_Z_MIN:
        failures.append("LONG_TAKER_ROBUST_Z_LT_2_00")
    return tuple(failures)


def _short_gate_failures(metrics: _D1EntryMetricsV0) -> tuple[str, ...]:
    failures: list[str] = []
    if metrics.previous_close < metrics.frozen_channel_lower:
        failures.append("SHORT_FIRST_CROSS_FAILED")
    if metrics.short_breakout_atr < _BREAKOUT_MIN_ATR:
        failures.append("SHORT_BREAKOUT_ATR_LT_0_10")
    if metrics.short_breakout_atr > _BREAKOUT_MAX_ATR:
        failures.append("SHORT_BREAKOUT_ATR_GT_0_50")
    if metrics.short_close_location < _CLOSE_LOCATION_MIN:
        failures.append("SHORT_CLOSE_LOCATION_LT_0_75")
    if not (metrics.latest_hour_close < metrics.ema20_hourly < metrics.ema50_hourly):
        failures.append("SHORT_STRICT_PRIOR_HOURLY_TREND_FAILED")
    failures.extend(_shared_gate_failures(metrics))
    if metrics.current_taker_imbalance > -_TAKER_IMBALANCE_MIN:
        failures.append("SHORT_TAKER_IMBALANCE_GT_NEG_0_20")
    if metrics.taker_robust_z > -_ROBUST_Z_MIN:
        failures.append("SHORT_TAKER_ROBUST_Z_GT_NEG_2_00")
    return tuple(failures)


def _shared_gate_failures(metrics: _D1EntryMetricsV0) -> list[str]:
    failures: list[str] = []
    if metrics.compression > _COMPRESSION_MAX:
        failures.append("COMPRESSION_GT_0_70")
    if metrics.current_expansion < _EXPANSION_MIN:
        failures.append("CURRENT_EXPANSION_LT_1_50")
    if metrics.current_expansion > _EXPANSION_MAX:
        failures.append("CURRENT_EXPANSION_GT_3_00")
    if metrics.atr_fraction < _ATR_FRACTION_MIN:
        failures.append("ATR_FRACTION_LT_0_0035")
    if metrics.prior_median_quote_volume < _PRIOR_QUOTE_VOLUME_MIN:
        failures.append("PRIOR_MEDIAN_QUOTE_VOLUME_LT_100000")
    if metrics.activity < _ACTIVITY_MIN:
        failures.append("ACTIVITY_LT_2_00")
    return failures


def _entry_decision(
    item: D1EntryInputV0,
    *,
    status: D1EntryStatusV0,
    side: D1SideV0 | None,
    reasons: tuple[str, ...],
    invalidation: str,
    metrics: _D1EntryMetricsV0 | None,
) -> D1EntryDecisionV0:
    metric_values: tuple[Decimal | None, ...]
    if metrics is None:
        metric_values = (None,) * 14
    else:
        metric_values = (
            metrics.signal_close,
            metrics.frozen_atr,
            metrics.frozen_channel_upper,
            metrics.frozen_channel_lower,
            metrics.compression,
            metrics.current_expansion,
            metrics.atr_fraction,
            metrics.prior_median_quote_volume,
            metrics.current_taker_imbalance,
            metrics.taker_robust_z,
            metrics.activity,
            metrics.latest_hour_close,
            metrics.ema20_hourly,
            metrics.ema50_hourly,
        )
    return D1EntryDecisionV0(
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        source_root_sha256=item.source_root_sha256,
        input_sha256=item.input_sha256,
        bar_open_ms=item.current_bar.open_ms,
        bar_close_ms=item.current_bar.close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        status=status,
        side=side,
        reasons=reasons,
        invalidation=invalidation,
        signal_close=metric_values[0],
        frozen_atr=metric_values[1],
        frozen_channel_upper=metric_values[2],
        frozen_channel_lower=metric_values[3],
        compression=metric_values[4],
        current_expansion=metric_values[5],
        atr_fraction=metric_values[6],
        prior_median_quote_volume=metric_values[7],
        current_taker_imbalance=metric_values[8],
        taker_robust_z=metric_values[9],
        activity=metric_values[10],
        latest_hour_close=metric_values[11],
        ema20_hourly=metric_values[12],
        ema50_hourly=metric_values[13],
        _factory_token=_ENTRY_DECISION_FACTORY_TOKEN,
    )


def _exit_authority_failures(item: D1ExitInputV0) -> tuple[str, ...]:
    failures: list[str] = []
    if not item.required_data_available:
        failures.append("REQUIRED_DATA_UNAVAILABLE")
    if not item.authority_continuity_declared:
        failures.append("CALLER_DECLARED_AUTHORITY_DISCONTINUITY")
    if len(item.bars_since_entry) > D1_HARD_HORIZON_BARS_V0:
        failures.append("EXIT_PATH_EXCEEDS_24_BAR_HORIZON")
    expected_first = item.position.first_exit_bar_open_ms
    if any(
        value.open_ms != expected_first + index * FIVE_MINUTE_MS_V2
        for index, value in enumerate(item.bars_since_entry)
    ):
        failures.append("EXIT_FIVE_MINUTE_SEQUENCE_NOT_CONTIGUOUS")
    if any(value.close_ms < item.position.entry_fill_ms for value in item.bars_since_entry):
        failures.append("EXIT_BAR_CLOSES_BEFORE_ENTRY_FILL")
    if any(not value.is_closed for value in item.bars_since_entry):
        failures.append("EXIT_BAR_NOT_FULLY_CLOSED")
    current = item.bars_since_entry[-1]
    if any(value.data_through_ms > current.close_ms for value in item.bars_since_entry):
        failures.append("EXIT_SOURCE_DATA_AFTER_CURRENT_CLOSE")
    if any(value.receipt_ms > item.decision_cutoff_ms for value in item.bars_since_entry):
        failures.append("EXIT_SOURCE_RECEIPT_AFTER_DECISION_CUTOFF")
    return tuple(failures)


def _exit_decision(
    item: D1ExitInputV0,
    *,
    status: D1ExitStatusV0,
    reason: D1ExitReasonV0,
    reasons: tuple[str, ...],
    invalidation: str,
    adverse: Decimal,
    structure: Decimal,
    profit: Decimal,
    interval_conclusive: bool,
) -> D1ExitDecisionV0:
    current = item.bars_since_entry[-1]
    action = (
        D1ExitActionV0.KEEP
        if status is D1ExitStatusV0.KEEP
        else (
            D1ExitActionV0.EXIT_LONG
            if item.position.side is D1SideV0.LONG
            else D1ExitActionV0.EXIT_SHORT
        )
    )
    return D1ExitDecisionV0(
        attempt_id=item.position.attempt_id,
        symbol=item.position.symbol,
        venue=item.position.venue,
        entry_event_id=item.position.entry_event_id,
        position_sha256=item.position.position_sha256,
        source_root_sha256=item.source_root_sha256,
        input_sha256=item.input_sha256,
        bar_open_ms=current.open_ms,
        bar_close_ms=current.close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        bars_elapsed=len(item.bars_since_entry),
        close_price=current.close_price,
        status=status,
        action=action,
        exit_reason=reason,
        reasons=reasons,
        invalidation=invalidation,
        adverse_boundary=adverse,
        structure_boundary=structure,
        profit_boundary=profit,
        interval_conclusive=interval_conclusive,
        _factory_token=_EXIT_DECISION_FACTORY_TOKEN,
    )


def _adverse_close(side: D1SideV0, close: Decimal, boundary: Decimal) -> bool:
    return close <= boundary if side is D1SideV0.LONG else close >= boundary


def _structure_failure(side: D1SideV0, close: Decimal, boundary: Decimal) -> bool:
    return close <= boundary if side is D1SideV0.LONG else close >= boundary


def _profit_close(side: D1SideV0, close: Decimal, boundary: Decimal) -> bool:
    return close >= boundary if side is D1SideV0.LONG else close <= boundary


def _true_ranges(
    anchor_close: Decimal,
    bars: tuple[D1FiveMinuteBarV0, ...],
) -> tuple[Decimal, ...]:
    previous_close = anchor_close
    ranges: list[Decimal] = []
    for value in bars:
        ranges.append(
            max(
                value.high_price - value.low_price,
                abs(value.high_price - previous_close),
                abs(value.low_price - previous_close),
            )
        )
        previous_close = value.close_price
    return tuple(ranges)


def _wilder_atr(true_ranges: tuple[Decimal, ...]) -> Decimal:
    if len(true_ranges) != D1_CALCULATION_BAR_COUNT_V0:
        raise _D1NonReadyArithmeticV0("ATR_TRUE_RANGE_COUNT_NOT_288")
    atr = sum(true_ranges[:D1_ATR_PERIOD_V0], Decimal(0)) / Decimal(D1_ATR_PERIOD_V0)
    for value in true_ranges[D1_ATR_PERIOD_V0:]:
        atr = (Decimal(13) * atr + value) / Decimal(D1_ATR_PERIOD_V0)
    return atr


def _taker_imbalance(value: D1FiveMinuteBarV0) -> Decimal:
    if value.quote_volume == 0:
        raise _D1NonReadyArithmeticV0("ZERO_TAKER_IMBALANCE_QUOTE_DENOMINATOR")
    return (Decimal(2) * value.taker_buy_quote_volume - value.quote_volume) / value.quote_volume


def _median(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise _D1NonReadyArithmeticV0("EMPTY_MEDIAN_WINDOW")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _ema(values: tuple[Decimal, ...], period: int) -> Decimal:
    if len(values) != D1_HOURLY_BAR_COUNT_V0 or period not in (20, 50):
        raise _D1NonReadyArithmeticV0("INVALID_HOURLY_EMA_WINDOW")
    period_decimal = Decimal(period)
    alpha = Decimal(2) / (period_decimal + Decimal(1))
    ema = sum(values[:period], Decimal(0)) / period_decimal
    for value in values[period:]:
        ema = alpha * value + (Decimal(1) - alpha) * ema
    return ema


def _five_minute_bar_document(
    value: D1FiveMinuteBarV0,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "close_ms": value.close_ms,
        "close_price": str(value.close_price),
        "data_through_ms": value.data_through_ms,
        "high_price": str(value.high_price),
        "is_closed": value.is_closed,
        "low_price": str(value.low_price),
        "open_ms": value.open_ms,
        "open_price": str(value.open_price),
        "quote_volume": str(value.quote_volume),
        "receipt_ms": value.receipt_ms,
        "schema_version": value.schema_version,
        "taker_buy_quote_volume": str(value.taker_buy_quote_volume),
    }
    if include_payload_hash:
        document["payload_sha256"] = value.payload_sha256
    return document


def _hourly_bar_document(
    value: D1HourlyBarV0,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "close_ms": value.close_ms,
        "close_price": str(value.close_price),
        "data_through_ms": value.data_through_ms,
        "is_closed": value.is_closed,
        "open_ms": value.open_ms,
        "receipt_ms": value.receipt_ms,
        "schema_version": value.schema_version,
    }
    if include_payload_hash:
        document["payload_sha256"] = value.payload_sha256
    return document


def _entry_input_document(
    value: D1EntryInputV0,
    *,
    include_input_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": value.attempt_id,
        "authority_binding": value.authority_binding,
        "causal_authority_bound": value.causal_authority_bound,
        "current_bar": _five_minute_bar_document(
            value.current_bar,
            include_payload_hash=True,
        ),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "hourly_bars": [
            _hourly_bar_document(item, include_payload_hash=True) for item in value.hourly_bars
        ],
        "paper_input_authorized": value.paper_input_authorized,
        "prior_bars": [
            _five_minute_bar_document(item, include_payload_hash=True) for item in value.prior_bars
        ],
        "required_fields_complete": value.required_fields_complete,
        "schema_version": value.schema_version,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_input_hash:
        document["input_sha256"] = value.input_sha256
    return document


def _entry_identity_document(value: D1EntryDecisionV0) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_open_ms": value.bar_open_ms,
        "family": "D1",
        "role": "ENTRY_DECISION",
        "rule_version": value.rule_version,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _entry_decision_document(
    value: D1EntryDecisionV0,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_entry_identity_document(value),
        "activity": _decimal_or_none(value.activity),
        "atr_fraction": _decimal_or_none(value.atr_fraction),
        "authority_binding": value.authority_binding,
        "bar_close_ms": value.bar_close_ms,
        "causal_authority_bound": value.causal_authority_bound,
        "compression": _decimal_or_none(value.compression),
        "current_expansion": _decimal_or_none(value.current_expansion),
        "current_taker_imbalance": _decimal_or_none(value.current_taker_imbalance),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "efficacy_claim": value.efficacy_claim,
        "ema20_hourly": _decimal_or_none(value.ema20_hourly),
        "ema50_hourly": _decimal_or_none(value.ema50_hourly),
        "event_id": value.event_id,
        "frozen_atr": _decimal_or_none(value.frozen_atr),
        "frozen_channel_lower": _decimal_or_none(value.frozen_channel_lower),
        "frozen_channel_upper": _decimal_or_none(value.frozen_channel_upper),
        "input_sha256": value.input_sha256,
        "invalidation": value.invalidation,
        "latest_hour_close": _decimal_or_none(value.latest_hour_close),
        "paper_entry_evaluated": value.paper_entry_evaluated,
        "paper_input_authorized": value.paper_input_authorized,
        "passed_gate_count_is_probability": value.passed_gate_count_is_probability,
        "prior_median_quote_volume": _decimal_or_none(value.prior_median_quote_volume),
        "probability_claim": value.probability_claim,
        "production_order_placement": value.production_order_placement,
        "reasons": list(value.reasons),
        "schema_version": value.schema_version,
        "side": None if value.side is None else value.side.value,
        "signal_close": _decimal_or_none(value.signal_close),
        "source_root_sha256": value.source_root_sha256,
        "status": value.status.value,
        "taker_robust_z": _decimal_or_none(value.taker_robust_z),
    }
    if include_payload_hash:
        document["payload_sha256"] = value.payload_sha256
    return document


def _position_document(value: D1PaperPositionAnchorV0) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "authority_binding": value.authority_binding,
        "entry_bar_close_ms": value.entry_bar_close_ms,
        "entry_bar_open_ms": value.entry_bar_open_ms,
        "entry_event_id": value.entry_event_id,
        "entry_fill_ms": value.entry_fill_ms,
        "entry_reference_kind": value.entry_reference_kind.value,
        "entry_vwap": str(value.entry_vwap),
        "entry_vwap_source_sha256": value.entry_vwap_source_sha256,
        "frozen_atr": str(value.frozen_atr),
        "frozen_channel_lower": str(value.frozen_channel_lower),
        "frozen_channel_upper": str(value.frozen_channel_upper),
        "first_exit_bar_open_ms": value.first_exit_bar_open_ms,
        "paper_fill_claim": value.paper_fill_claim,
        "production_order_placement": value.production_order_placement,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "side": value.side.value,
        "signal_close": str(value.signal_close),
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _exit_input_document(
    value: D1ExitInputV0,
    *,
    include_input_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "authority_binding": value.authority_binding,
        "authority_continuity_declared": value.authority_continuity_declared,
        "bars_since_entry": [
            _five_minute_bar_document(item, include_payload_hash=True)
            for item in value.bars_since_entry
        ],
        "causal_authority_bound": value.causal_authority_bound,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "position": {
            **_position_document(value.position),
            "position_sha256": value.position.position_sha256,
        },
        "required_data_available": value.required_data_available,
        "schema_version": value.schema_version,
        "source_root_sha256": value.source_root_sha256,
    }
    if include_input_hash:
        document["input_sha256"] = value.input_sha256
    return document


def _exit_identity_document(value: D1ExitDecisionV0) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_open_ms": value.bar_open_ms,
        "entry_event_id": value.entry_event_id,
        "family": "D1",
        "role": "EXIT_DECISION",
        "rule_version": value.rule_version,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _exit_decision_document(
    value: D1ExitDecisionV0,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_exit_identity_document(value),
        "action": value.action.value,
        "adverse_boundary": str(value.adverse_boundary),
        "authority_binding": value.authority_binding,
        "bar_close_ms": value.bar_close_ms,
        "bars_elapsed": value.bars_elapsed,
        "causal_authority_bound": value.causal_authority_bound,
        "close_price": str(value.close_price),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "efficacy_claim": value.efficacy_claim,
        "event_id": value.event_id,
        "exit_reason": value.exit_reason.value,
        "input_sha256": value.input_sha256,
        "interval_conclusive": value.interval_conclusive,
        "invalidation": value.invalidation,
        "paper_exit_fill_claim": value.paper_exit_fill_claim,
        "position_sha256": value.position_sha256,
        "probability_claim": value.probability_claim,
        "production_order_placement": value.production_order_placement,
        "profit_boundary": str(value.profit_boundary),
        "reasons": list(value.reasons),
        "schema_version": value.schema_version,
        "source_root_sha256": value.source_root_sha256,
        "status": value.status.value,
        "structure_boundary": str(value.structure_boundary),
    }
    if include_payload_hash:
        document["payload_sha256"] = value.payload_sha256
    return document


def _validate_entry_decision_state(value: D1EntryDecisionV0) -> None:
    metrics = (
        value.signal_close,
        value.frozen_atr,
        value.frozen_channel_upper,
        value.frozen_channel_lower,
        value.compression,
        value.current_expansion,
        value.atr_fraction,
        value.prior_median_quote_volume,
        value.current_taker_imbalance,
        value.taker_robust_z,
        value.activity,
        value.latest_hour_close,
        value.ema20_hourly,
        value.ema50_hourly,
    )
    for metric in metrics:
        if metric is not None:
            _validate_finite_decimal(metric, "entry metric")
    if value.status is D1EntryStatusV0.SIGNAL:
        if value.side is None or any(metric is None for metric in metrics):
            raise D1ScefbContractErrorV0("SIGNAL requires side and every frozen metric")
    elif value.side is not None:
        raise D1ScefbContractErrorV0("non-SIGNAL entry decisions cannot carry a side")
    if value.status is D1EntryStatusV0.INCONCLUSIVE and any(
        metric is not None for metric in metrics
    ):
        raise D1ScefbContractErrorV0("INCONCLUSIVE decisions cannot carry partial metrics")


def _validate_exit_decision_state(value: D1ExitDecisionV0) -> None:
    if type(value.interval_conclusive) is not bool:
        raise D1ScefbContractErrorV0("interval_conclusive must be a boolean")
    if value.status is D1ExitStatusV0.KEEP:
        if value.action is not D1ExitActionV0.KEEP or value.exit_reason is not D1ExitReasonV0.KEEP:
            raise D1ScefbContractErrorV0("KEEP status requires KEEP action and reason")
        if not value.interval_conclusive:
            raise D1ScefbContractErrorV0("KEEP interval must be conclusive")
        return
    if value.action is D1ExitActionV0.KEEP or value.exit_reason is D1ExitReasonV0.KEEP:
        raise D1ScefbContractErrorV0("exit status requires an exit action and reason")
    if value.status is D1ExitStatusV0.INCONCLUSIVE_EXIT:
        if value.exit_reason is not D1ExitReasonV0.AUTHORITY_LOSS or value.interval_conclusive:
            raise D1ScefbContractErrorV0(
                "INCONCLUSIVE_EXIT requires authority loss and an inconclusive interval"
            )
    elif not value.interval_conclusive:
        raise D1ScefbContractErrorV0("ordinary exits must be conclusive")


def _validate_common_identity(
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    source_root_sha256: str,
    input_sha256: str,
) -> None:
    _validate_identity(attempt_id, "attempt_id")
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise D1ScefbContractErrorV0("D1 decision requires USD-M Futures")
    _validate_sha256(source_root_sha256, "source_root_sha256")
    _validate_sha256(input_sha256, "input_sha256")


def _validate_decision_clock(open_ms: int, close_ms: int, cutoff_ms: int) -> None:
    _validate_bar_slot(open_ms, close_ms, FIVE_MINUTE_MS_V2, "decision 5m")
    if cutoff_ms != close_ms + DECISION_DELAY_MS_V2:
        raise D1ScefbContractErrorV0("decision cutoff must equal close + 2,001 ms")


def _validate_bar_slot(open_ms: int, close_ms: int, span_ms: int, label: str) -> None:
    _validate_nonnegative_int(open_ms, "open_ms")
    _validate_nonnegative_int(close_ms, "close_ms")
    if open_ms % span_ms != 0 or close_ms != open_ms + span_ms - 1:
        raise D1ScefbContractErrorV0(f"{label} bar is not UTC aligned and complete-shaped")


def _validate_ohlc(
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
) -> None:
    for value, field_name in (
        (open_price, "open_price"),
        (high_price, "high_price"),
        (low_price, "low_price"),
        (close_price, "close_price"),
    ):
        _validate_positive_decimal(value, field_name)
    if high_price < max(open_price, low_price, close_price):
        raise D1ScefbContractErrorV0("high_price is below another OHLC value")
    if low_price > min(open_price, high_price, close_price):
        raise D1ScefbContractErrorV0("low_price is above another OHLC value")


def _validate_positive_decimal(value: Decimal, field_name: str) -> None:
    _validate_finite_decimal(value, field_name)
    if value <= 0:
        raise D1ScefbContractErrorV0(f"{field_name} must be positive")


def _validate_nonnegative_decimal(value: Decimal, field_name: str) -> None:
    _validate_finite_decimal(value, field_name)
    if value < 0:
        raise D1ScefbContractErrorV0(f"{field_name} must be nonnegative")


def _validate_finite_decimal(value: Decimal, field_name: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise D1ScefbContractErrorV0(f"{field_name} must be a finite Decimal")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise D1ScefbContractErrorV0(f"{field_name} must be a nonnegative integer")


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise D1ScefbContractErrorV0(f"{field_name} must be a bounded normalized identity")


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise D1ScefbContractErrorV0("symbol must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D1ScefbContractErrorV0(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_reasons(value: tuple[str, ...]) -> None:
    if type(value) is not tuple or not value:
        raise D1ScefbContractErrorV0("reasons must be a non-empty tuple")
    for reason in value:
        _validate_identity(reason, "reason")


def _require_exact_tuple(value: object, item_type: type[object], field_name: str) -> None:
    if type(value) is not tuple or any(type(item) is not item_type for item in value):
        raise D1ScefbContractErrorV0(
            f"{field_name} must be an immutable tuple of exact sealed values"
        )


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
