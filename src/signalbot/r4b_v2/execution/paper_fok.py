from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from fractions import Fraction
from typing import Final, Protocol

from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import validate_decision_bar_v2

PAPER_FOK_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.3.1_PAPER_FOK_ENTRY"
PRIMARY_PAPER_TARGET_DELAY_MS_V2: Final = 10_000
PRIMARY_DEPTH_HAIRCUT_V2: Final = Decimal("0.50")
PAPER_PRICE_CAP_RATE_V2: Final = Decimal("0.0010")
MARK_PRICE_MAX_STALENESS_MS_V2: Final = 2_000

_EVENT_ID_DOMAIN: Final = b"R4B_PAPER_FOK_ENTRY_EVENT_V2\0"
_PAYLOAD_DOMAIN: Final = b"R4B_PAPER_FOK_ENTRY_PAYLOAD_V2\0"
_EVIDENCE_DOMAIN: Final = b"R4B_PAPER_FOK_ENTRY_EVIDENCE_V2\0"
_REPLAY_ROOT_DOMAIN: Final = b"R4B_PAPER_FOK_REPLAY_ROOT_V2\0"
_REGISTRY_CHECKPOINT_DOMAIN: Final = b"R4B_PAPER_FOK_REGISTRY_CHECKPOINT_V2\0"
_CERTIFICATE_DOMAIN: Final = b"R4B_PAPER_FOK_FULL_FILL_CERTIFICATE_V2\0"
_REGISTRY_STATE_SCHEMA: Final = "r4b_paper_fok_registry_state_v2"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_DECISION_FACTORY_TOKEN: Final = object()
_CERTIFICATE_FACTORY_TOKEN: Final = object()


class PaperFokContractErrorV2(ValueError):
    """Raised when PAPER execution evidence violates the frozen V2 contract."""


class PaperFokSideV2(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PaperFokEntryStatusV2(StrEnum):
    CLOSURE_PENDING = "CLOSURE_PENDING"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    INCONCLUSIVE_FILTER = "INCONCLUSIVE_FILTER"
    ADMITTED_PAPER_IOC_NO_FILL = "ADMITTED_PAPER_IOC_NO_FILL"
    NOT_ADMITTED_PAPER_CAPACITY = "NOT_ADMITTED_PAPER_CAPACITY"
    ADMITTED_EXECUTED_FULL_QUANTITY = "ADMITTED_EXECUTED_FULL_QUANTITY"


class PaperFokClosureMethodV2(StrEnum):
    PENDING = "PENDING"
    CONTIGUOUS_SUCCESSOR = "CONTIGUOUS_SUCCESSOR"
    QUIET_REST_EQUAL = "QUIET_REST_EQUAL"
    INVALID = "INVALID"


class PaperFokRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


class PaperFokInconclusiveCauseV2(StrEnum):
    INCONCLUSIVE_DATA_SCHEMA = "INCONCLUSIVE_DATA_SCHEMA"
    INCONCLUSIVE_DATA_SEQUENCE = "INCONCLUSIVE_DATA_SEQUENCE"
    INCONCLUSIVE_DATA_BOOK = "INCONCLUSIVE_DATA_BOOK"
    INCONCLUSIVE_DATA_CAUSAL = "INCONCLUSIVE_DATA_CAUSAL"
    INCONCLUSIVE_FILTER = "INCONCLUSIVE_FILTER"
    INCONCLUSIVE_EXECUTION_RULE = "INCONCLUSIVE_EXECUTION_RULE"
    INCONCLUSIVE_CLOSURE = "INCONCLUSIVE_CLOSURE"


class _EvidenceRowV2(Protocol):
    @property
    def symbol(self) -> str: ...

    @property
    def venue(self) -> VenueV2: ...

    @property
    def promoting_plan_sha256(self) -> str: ...

    @property
    def source_root_sha256(self) -> str: ...

    @property
    def schema_sha256(self) -> str: ...

    @property
    def source_kind(self) -> str: ...


@dataclass(frozen=True, slots=True)
class RawQuantityFilterV2:
    """One raw USD-M quantity filter; zero fields are disabled constraints."""

    min_qty: Decimal
    max_qty: Decimal
    step_size: Decimal

    def __post_init__(self) -> None:
        for value, name in (
            (self.min_qty, "min_qty"),
            (self.max_qty, "max_qty"),
            (self.step_size, "step_size"),
        ):
            _validate_decimal(value, name, allow_zero=True)

    @property
    def fully_disabled(self) -> bool:
        return self.min_qty == self.max_qty == self.step_size == 0


@dataclass(frozen=True, slots=True)
class CommonQuantityGridV2:
    """Exact intersection of enabled LOT_SIZE and MARKET_LOT_SIZE constraints.

    Decimal filter values are integerized at one power-of-ten scale. Congruences
    use each filter's ``minQty`` as their origin and are combined with generalized
    CRT. Per-level flooring uses the resulting residue, modulus, and all bounds;
    it is never replaced by a zero-origin quantum shortcut.
    """

    scale: int
    residue_units: int
    modulus_units: int
    minimum_units: int
    maximum_units: int
    first_legal_units: int

    def __post_init__(self) -> None:
        if type(self.scale) is not int or self.scale < 1:
            raise PaperFokContractErrorV2("grid scale must be a positive integer")
        if type(self.residue_units) is not int or self.residue_units < 0:
            raise PaperFokContractErrorV2("grid residue must be nonnegative")
        if type(self.modulus_units) is not int or self.modulus_units < 1:
            raise PaperFokContractErrorV2("grid modulus must be positive")
        if self.residue_units >= self.modulus_units:
            raise PaperFokContractErrorV2("grid residue must be below its modulus")
        if (
            type(self.minimum_units) is not int
            or type(self.maximum_units) is not int
            or self.minimum_units < 0
            or self.maximum_units < self.minimum_units
        ):
            raise PaperFokContractErrorV2("grid bounds are invalid")
        if not self.is_legal_units(self.first_legal_units):
            raise PaperFokContractErrorV2("first legal quantity contradicts the grid")

    @property
    def quantum(self) -> Decimal:
        return _units_to_decimal(self.modulus_units, self.scale)

    @property
    def minimum(self) -> Decimal:
        return _units_to_decimal(self.minimum_units, self.scale)

    @property
    def maximum(self) -> Decimal:
        return _units_to_decimal(self.maximum_units, self.scale)

    @property
    def first_legal(self) -> Decimal:
        return _units_to_decimal(self.first_legal_units, self.scale)

    def is_legal_units(self, quantity_units: int) -> bool:
        return (
            type(quantity_units) is int
            and self.minimum_units <= quantity_units <= self.maximum_units
            and (quantity_units - self.residue_units) % self.modulus_units == 0
        )

    def is_legal(self, quantity: Decimal) -> bool:
        units = _decimal_to_units_exact(quantity, self.scale)
        return units is not None and self.is_legal_units(units)

    def floor_capacity_per_level(self, quantity: Decimal) -> Decimal:
        """Floor one level to the largest bounded CRT-valid point independently."""

        return self.floor_legal_total(quantity)

    def floor_legal_total(self, quantity: Decimal) -> Decimal:
        """Return the greatest globally legal order quantity at or below capacity."""

        units = min(_decimal_floor_to_units(quantity, self.scale), self.maximum_units)
        if units < self.first_legal_units:
            return Decimal(0)
        steps = (units - self.residue_units) // self.modulus_units
        candidate = self.residue_units + steps * self.modulus_units
        if candidate < self.minimum_units:
            return Decimal(0)
        return _units_to_decimal(candidate, self.scale)


def intersect_quantity_filters_v2(
    lot_size: RawQuantityFilterV2 | None,
    market_lot_size: RawQuantityFilterV2 | None,
) -> CommonQuantityGridV2:
    """Intersect all enabled bounds and minQty-origin step congruences exactly."""

    raw_filters = tuple(
        value
        for value in (lot_size, market_lot_size)
        if value is not None and not value.fully_disabled
    )
    if not raw_filters:
        raise PaperFokContractErrorV2("no enabled USD-M quantity filter exists")
    grid_decimals = tuple(value for item in raw_filters for value in (item.min_qty, item.step_size))
    scale = 10 ** max(_decimal_places(value) for value in grid_decimals)
    minima = [_decimal_to_units_required(item.min_qty, scale) for item in raw_filters]
    maxima = [
        _decimal_floor_to_units(item.max_qty, scale) for item in raw_filters if item.max_qty > 0
    ]
    minimum_units = max(minima, default=0)
    if not maxima:
        raise PaperFokContractErrorV2("enabled filters require a finite maxQty")
    maximum_units = min(maxima)
    if maximum_units < minimum_units:
        raise PaperFokContractErrorV2("quantity-filter min/max intersection is empty")

    congruences = [
        (
            _decimal_to_units_required(item.min_qty, scale),
            _decimal_to_units_required(item.step_size, scale),
        )
        for item in raw_filters
        if item.step_size > 0
    ]
    if not congruences:
        raise PaperFokContractErrorV2("enabled filters expose no nonzero step grid")
    residue, modulus = congruences[0]
    residue %= modulus
    for next_residue, next_modulus in congruences[1:]:
        residue, modulus = _combine_congruences(
            residue,
            modulus,
            next_residue % next_modulus,
            next_modulus,
        )
    first_legal = residue
    if first_legal < minimum_units:
        first_legal += _ceil_div(minimum_units - first_legal, modulus) * modulus
    if first_legal > maximum_units:
        raise PaperFokContractErrorV2("quantity-filter CRT has no legal bounded value")
    return CommonQuantityGridV2(
        scale=scale,
        residue_units=residue,
        modulus_units=modulus,
        minimum_units=minimum_units,
        maximum_units=maximum_units,
        first_legal_units=first_legal,
    )


def _combine_congruences(
    residue_a: int,
    modulus_a: int,
    residue_b: int,
    modulus_b: int,
) -> tuple[int, int]:
    divisor = math.gcd(modulus_a, modulus_b)
    difference = residue_b - residue_a
    if difference % divisor:
        raise PaperFokContractErrorV2("quantity-filter step grids have no congruence")
    reduced_a = modulus_a // divisor
    reduced_b = modulus_b // divisor
    if reduced_b == 1:
        multiplier = 0
    else:
        multiplier = (difference // divisor * pow(reduced_a, -1, reduced_b)) % reduced_b
    combined_modulus = modulus_a * reduced_b
    combined_residue = (residue_a + modulus_a * multiplier) % combined_modulus
    return combined_residue, combined_modulus


def _ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


def _decimal_places(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):  # finite Decimal validated by every caller
        raise PaperFokContractErrorV2("nonfinite Decimal has no grid scale")
    return max(0, -exponent)


def _decimal_to_units_required(value: Decimal, scale: int) -> int:
    units = _decimal_to_units_exact(value, scale)
    if units is None:  # pragma: no cover - scale is derived from every input
        raise PaperFokContractErrorV2("Decimal cannot be represented on grid scale")
    return units


def _decimal_to_units_exact(value: Decimal, scale: int) -> int | None:
    numerator, denominator = _decimal_ratio(value)
    scaled_numerator = numerator * scale
    units, remainder = divmod(scaled_numerator, denominator)
    if remainder:
        return None
    return units


def _decimal_floor_to_units(value: Decimal, scale: int) -> int:
    _validate_decimal(value, "quantity", allow_zero=True)
    numerator, denominator = _decimal_ratio(value)
    return numerator * scale // denominator


def _decimal_ratio(value: Decimal) -> tuple[int, int]:
    """Return an exact nonnegative integer ratio without Decimal arithmetic."""

    _validate_decimal(value, "Decimal value", allow_zero=True)
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):  # finite Decimal validated above
        raise PaperFokContractErrorV2("nonfinite Decimal has no exact ratio")
    if sign:
        raise PaperFokContractErrorV2("Decimal value must be nonnegative")
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    if exponent >= 0:
        return coefficient * 10**exponent, 1
    return coefficient, 10 ** (-exponent)


def _units_to_decimal(units: int, scale: int) -> Decimal:
    if type(units) is not int or units < 0:
        raise PaperFokContractErrorV2("scaled units must be nonnegative")
    if type(scale) is not int or scale < 1:
        raise PaperFokContractErrorV2("scale must be a positive integer")
    exponent = 0
    remainder = scale
    while remainder > 1 and remainder % 10 == 0:
        exponent -= 1
        remainder //= 10
    if remainder != 1:
        raise PaperFokContractErrorV2("scale must be a power of ten")
    digits = tuple(int(character) for character in str(units))
    return Decimal((0, digits, exponent))


def _validate_decimal(value: Decimal, field_name: str, *, allow_zero: bool) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise PaperFokContractErrorV2(f"{field_name} must be a finite Decimal")
    if value < 0 or (not allow_zero and value == 0):
        qualifier = "nonnegative" if allow_zero else "positive"
        raise PaperFokContractErrorV2(f"{field_name} must be {qualifier}")


@dataclass(frozen=True, slots=True)
class PaperFokLineageV2:
    """Frozen roots expected on every execution input row."""

    promoting_plan_sha256: str
    source_root_sha256: str
    depth_snapshot_schema_sha256: str
    standard_depth_schema_sha256: str
    mark_schema_sha256: str
    exchange_info_schema_sha256: str
    health_schema_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "promoting_plan_sha256",
            "source_root_sha256",
            "depth_snapshot_schema_sha256",
            "standard_depth_schema_sha256",
            "mark_schema_sha256",
            "exchange_info_schema_sha256",
            "health_schema_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)

    @property
    def lineage_sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(_lineage_document(self))).hexdigest()


@dataclass(frozen=True, slots=True)
class DepthLevelV2:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        _validate_decimal(self.price, "price", allow_zero=False)
        _validate_decimal(self.quantity, "quantity", allow_zero=True)


@dataclass(frozen=True, slots=True)
class FuturesDepthSnapshotV2:
    """Raw public REST bootstrap for one standard USD-M local book."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    response_completion_ms: int
    last_update_id: int
    depth_limit: int
    bids: tuple[DepthLevelV2, ...]
    asks: tuple[DepthLevelV2, ...]
    source_kind: str = field(init=False, default="FUTURES_REST_DEPTH_SNAPSHOT")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        _validate_nonnegative_int(self.response_completion_ms, "response_completion_ms")
        _validate_nonnegative_int(self.last_update_id, "last_update_id")
        if type(self.depth_limit) is not int or self.depth_limit < 1:
            raise PaperFokContractErrorV2("depth_limit must be a positive integer")
        _validate_levels(self.bids, "bids", allow_zero_quantity=False)
        _validate_levels(self.asks, "asks", allow_zero_quantity=False)
        if len(self.bids) > self.depth_limit or len(self.asks) > self.depth_limit:
            raise PaperFokContractErrorV2("snapshot side exceeds depth_limit")
        if tuple(sorted(self.bids, key=lambda level: level.price, reverse=True)) != self.bids:
            raise PaperFokContractErrorV2("snapshot bids must be strictly best-to-worst")
        if tuple(sorted(self.asks, key=lambda level: level.price)) != self.asks:
            raise PaperFokContractErrorV2("snapshot asks must be strictly best-to-worst")


@dataclass(frozen=True, slots=True)
class FuturesStandardDepthEventV2:
    """One raw standard diff-depth row; RPI and bookTicker have no representation."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    pair: str
    routing_status: int
    event_time_ms: int
    transaction_time_ms: int
    receipt_completion_ms: int
    ingest_seq: int
    previous_same_stream_ingest_seq: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    bids: tuple[DepthLevelV2, ...]
    asks: tuple[DepthLevelV2, ...]
    source_kind: str = field(init=False, default="STANDARD_DIFF_DEPTH_100MS")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        _validate_symbol_shape(self.pair, "pair")
        if type(self.routing_status) is not int:
            raise PaperFokContractErrorV2("routing_status must be an integer")
        for field_name in (
            "event_time_ms",
            "transaction_time_ms",
            "receipt_completion_ms",
            "first_update_id",
            "final_update_id",
            "previous_final_update_id",
            "previous_same_stream_ingest_seq",
        ):
            _validate_nonnegative_int(getattr(self, field_name), field_name)
        if type(self.ingest_seq) is not int or self.ingest_seq < 1:
            raise PaperFokContractErrorV2("ingest_seq must be a positive integer")
        if self.first_update_id > self.final_update_id:
            raise PaperFokContractErrorV2("depth U cannot exceed u")
        _validate_levels(self.bids, "bids", allow_zero_quantity=True)
        _validate_levels(self.asks, "asks", allow_zero_quantity=True)


@dataclass(frozen=True, slots=True)
class FuturesDepthContinuityWitnessV2:
    """Price-free projection of the first post-target depth event."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    pair: str
    routing_status: int
    event_time_ms: int
    transaction_time_ms: int
    receipt_completion_ms: int
    ingest_seq: int
    previous_same_stream_ingest_seq: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    source_kind: str = field(init=False, default="STANDARD_DIFF_DEPTH_CONTINUITY_ONLY")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        _validate_symbol_shape(self.pair, "pair")
        if type(self.routing_status) is not int:
            raise PaperFokContractErrorV2("routing_status must be an integer")
        for field_name in (
            "event_time_ms",
            "transaction_time_ms",
            "receipt_completion_ms",
            "first_update_id",
            "final_update_id",
            "previous_final_update_id",
            "previous_same_stream_ingest_seq",
        ):
            _validate_nonnegative_int(getattr(self, field_name), field_name)
        if type(self.ingest_seq) is not int or self.ingest_seq < 1:
            raise PaperFokContractErrorV2("ingest_seq must be a positive integer")
        if self.first_update_id > self.final_update_id:
            raise PaperFokContractErrorV2("continuity U cannot exceed u")

    @classmethod
    def from_event(
        cls,
        event: FuturesStandardDepthEventV2,
    ) -> FuturesDepthContinuityWitnessV2:
        if not isinstance(event, FuturesStandardDepthEventV2):
            raise PaperFokContractErrorV2("continuity source must be standard depth")
        return cls(
            symbol=event.symbol,
            venue=event.venue,
            promoting_plan_sha256=event.promoting_plan_sha256,
            source_root_sha256=event.source_root_sha256,
            schema_sha256=event.schema_sha256,
            pair=event.pair,
            routing_status=event.routing_status,
            event_time_ms=event.event_time_ms,
            transaction_time_ms=event.transaction_time_ms,
            receipt_completion_ms=event.receipt_completion_ms,
            ingest_seq=event.ingest_seq,
            previous_same_stream_ingest_seq=event.previous_same_stream_ingest_seq,
            first_update_id=event.first_update_id,
            final_update_id=event.final_update_id,
            previous_final_update_id=event.previous_final_update_id,
        )


@dataclass(frozen=True, slots=True)
class QuietRestSnapshotEvidenceV2:
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    response_completion_ms: int
    last_update_id: int
    source_kind: str = field(init=False, default="FINAL_QUIET_REST_DEPTH_SNAPSHOT")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        _validate_nonnegative_int(self.response_completion_ms, "response_completion_ms")
        _validate_nonnegative_int(self.last_update_id, "last_update_id")


@dataclass(frozen=True, slots=True)
class ContinuousBookHealthEvidenceV2:
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    interval_start_local_ms: int
    interval_end_local_ms: int
    generation: int
    disconnect_count: int
    parser_error_count: int
    queue_drop_count: int
    sequence_gap_count: int
    source_kind: str = field(init=False, default="BOOK_HEALTH")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        for field_name in (
            "interval_start_local_ms",
            "interval_end_local_ms",
            "disconnect_count",
            "parser_error_count",
            "queue_drop_count",
            "sequence_gap_count",
        ):
            _validate_nonnegative_int(getattr(self, field_name), field_name)
        if type(self.generation) is not int or self.generation < 1:
            raise PaperFokContractErrorV2("generation must be a positive integer")
        if self.interval_end_local_ms < self.interval_start_local_ms:
            raise PaperFokContractErrorV2("health interval is reversed")

    @property
    def continuous(self) -> bool:
        return not any(
            (
                self.disconnect_count,
                self.parser_error_count,
                self.queue_drop_count,
                self.sequence_gap_count,
            )
        )


@dataclass(frozen=True, slots=True)
class PaperFokClosureEvidenceV2:
    closure_grace_end_local_ms: int
    finalization_grace_binding_sha256: str
    finalized_through_local_ms: int
    successor_candidates: tuple[FuturesDepthContinuityWitnessV2, ...] = ()
    quiet_rest_snapshot: QuietRestSnapshotEvidenceV2 | None = None
    continuous_health: ContinuousBookHealthEvidenceV2 | None = None

    def __post_init__(self) -> None:
        _validate_nonnegative_int(
            self.closure_grace_end_local_ms,
            "closure_grace_end_local_ms",
        )
        _validate_sha256(
            self.finalization_grace_binding_sha256,
            "finalization_grace_binding_sha256",
        )
        _validate_nonnegative_int(
            self.finalized_through_local_ms,
            "finalized_through_local_ms",
        )
        if type(self.successor_candidates) is not tuple or any(
            not isinstance(value, FuturesDepthContinuityWitnessV2)
            for value in self.successor_candidates
        ):
            raise PaperFokContractErrorV2(
                "successor_candidates must be an immutable continuity tuple"
            )
        if self.quiet_rest_snapshot is not None and not isinstance(
            self.quiet_rest_snapshot,
            QuietRestSnapshotEvidenceV2,
        ):
            raise PaperFokContractErrorV2("quiet snapshot has the wrong type")
        if self.continuous_health is not None and not isinstance(
            self.continuous_health,
            ContinuousBookHealthEvidenceV2,
        ):
            raise PaperFokContractErrorV2("health evidence has the wrong type")
        if self.successor_candidates and (
            self.quiet_rest_snapshot is not None or self.continuous_health is not None
        ):
            raise PaperFokContractErrorV2("successor and quiet-book closure paths cannot be mixed")
        if (self.quiet_rest_snapshot is None) != (self.continuous_health is None):
            raise PaperFokContractErrorV2(
                "quiet-book closure requires both REST and continuous-health evidence"
            )


@dataclass(frozen=True, slots=True)
class CausalMarkPriceEvidenceV2:
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    pair: str
    routing_status: int
    mark_price: Decimal
    event_time_ms: int
    receipt_completion_ms: int
    source_kind: str = field(init=False, default="MARK_PRICE_1S")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        _validate_symbol_shape(self.pair, "pair")
        if type(self.routing_status) is not int:
            raise PaperFokContractErrorV2("routing_status must be an integer")
        _validate_decimal(self.mark_price, "mark_price", allow_zero=False)
        _validate_nonnegative_int(self.event_time_ms, "event_time_ms")
        _validate_nonnegative_int(self.receipt_completion_ms, "receipt_completion_ms")


@dataclass(frozen=True, slots=True)
class FuturesExchangeInfoEvidenceV2:
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    schema_sha256: str
    response_completion_ms: int
    version_valid_from_local_ms: int
    version_valid_through_local_ms: int
    applicable_filter_inventory_complete: bool
    tick_size: Decimal
    min_price: Decimal
    max_price: Decimal
    percent_price_multiplier_down: Decimal | None
    percent_price_multiplier_up: Decimal | None
    market_take_bound: Decimal
    min_notional: Decimal
    max_notional: Decimal
    lot_size: RawQuantityFilterV2 | None
    market_lot_size: RawQuantityFilterV2 | None
    source_kind: str = field(init=False, default="FUTURES_EXCHANGE_INFO")

    def __post_init__(self) -> None:
        _validate_row_identity(self)
        for field_name in (
            "response_completion_ms",
            "version_valid_from_local_ms",
            "version_valid_through_local_ms",
        ):
            _validate_nonnegative_int(getattr(self, field_name), field_name)
        if self.version_valid_through_local_ms < self.version_valid_from_local_ms:
            raise PaperFokContractErrorV2("exchange-info validity interval is reversed")
        if type(self.applicable_filter_inventory_complete) is not bool:
            raise PaperFokContractErrorV2("applicable_filter_inventory_complete must be boolean")
        _validate_decimal(self.tick_size, "tick_size", allow_zero=False)
        for value, field_name in (
            (self.min_price, "min_price"),
            (self.max_price, "max_price"),
            (self.min_notional, "min_notional"),
            (self.max_notional, "max_notional"),
        ):
            _validate_decimal(value, field_name, allow_zero=True)
        if self.max_price > 0 and self.min_price > self.max_price:
            raise PaperFokContractErrorV2("PRICE_FILTER min exceeds max")
        if self.max_notional > 0 and self.min_notional > self.max_notional:
            raise PaperFokContractErrorV2("notional-filter min exceeds max")
        if (self.percent_price_multiplier_down is None) != (
            self.percent_price_multiplier_up is None
        ):
            raise PaperFokContractErrorV2(
                "percent-price multipliers must both be present or absent"
            )
        if self.percent_price_multiplier_down is not None:
            assert self.percent_price_multiplier_up is not None
            _validate_decimal(
                self.percent_price_multiplier_down,
                "percent_price_multiplier_down",
                allow_zero=False,
            )
            _validate_decimal(
                self.percent_price_multiplier_up,
                "percent_price_multiplier_up",
                allow_zero=False,
            )
            if self.percent_price_multiplier_down > self.percent_price_multiplier_up:
                raise PaperFokContractErrorV2(
                    "percent-price lower multiplier exceeds upper multiplier"
                )
        _validate_decimal(
            self.market_take_bound,
            "market_take_bound",
            allow_zero=True,
        )
        if self.market_take_bound > 1:
            raise PaperFokContractErrorV2("market_take_bound cannot exceed one")
        for value, field_name in (
            (self.lot_size, "lot_size"),
            (self.market_lot_size, "market_lot_size"),
        ):
            if value is not None and not isinstance(value, RawQuantityFilterV2):
                raise PaperFokContractErrorV2(f"{field_name} must be RawQuantityFilterV2 or None")


@dataclass(frozen=True, slots=True)
class PaperFokEntryInputV2:
    attempt_id: str
    signal_event_id: str
    symbol: str
    venue: VenueV2
    lineage: PaperFokLineageV2
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    target_cursor: CausalTargetCursorV2
    target_state_last_ingest_seq: int
    side: PaperFokSideV2
    requested_quantity: Decimal
    snapshot: FuturesDepthSnapshotV2
    pre_target_depth_events: tuple[FuturesStandardDepthEventV2, ...]
    closure: PaperFokClosureEvidenceV2
    mark: CausalMarkPriceEvidenceV2
    exchange_info: FuturesExchangeInfoEvidenceV2

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.signal_event_id, "signal_event_id")
        _validate_symbol_shape(self.symbol, "symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise PaperFokContractErrorV2("PAPER FOK entry is USD-M Futures only")
        if not isinstance(self.lineage, PaperFokLineageV2):
            raise PaperFokContractErrorV2("lineage must be PaperFokLineageV2")
        try:
            validate_decision_bar_v2(
                self.bar_open_ms,
                self.bar_close_ms,
                self.decision_cutoff_ms,
            )
        except ValueError as exc:
            raise PaperFokContractErrorV2(str(exc)) from exc
        if not isinstance(self.target_cursor, CausalTargetCursorV2):
            raise PaperFokContractErrorV2(
                "target_cursor must be the actionability CausalTargetCursorV2"
            )
        _validate_nonnegative_int(
            self.target_state_last_ingest_seq,
            "target_state_last_ingest_seq",
        )
        if self.target_cursor.decision_cutoff_ms != self.decision_cutoff_ms:
            raise PaperFokContractErrorV2(
                "target cursor decision cutoff differs from the PAPER decision"
            )
        if not isinstance(self.side, PaperFokSideV2):
            raise PaperFokContractErrorV2("side must be BUY or SELL")
        _validate_decimal(
            self.requested_quantity,
            "requested_quantity",
            allow_zero=False,
        )
        if not isinstance(self.snapshot, FuturesDepthSnapshotV2):
            raise PaperFokContractErrorV2("snapshot must be FuturesDepthSnapshotV2")
        if type(self.pre_target_depth_events) is not tuple or any(
            not isinstance(event, FuturesStandardDepthEventV2)
            for event in self.pre_target_depth_events
        ):
            raise PaperFokContractErrorV2(
                "pre_target_depth_events must be an immutable standard-depth tuple"
            )
        if not isinstance(self.closure, PaperFokClosureEvidenceV2):
            raise PaperFokContractErrorV2("closure has the wrong type")
        if not isinstance(self.mark, CausalMarkPriceEvidenceV2):
            raise PaperFokContractErrorV2("mark has the wrong type")
        if not isinstance(self.exchange_info, FuturesExchangeInfoEvidenceV2):
            raise PaperFokContractErrorV2("exchange_info has the wrong type")

    @property
    def target_venue_ms(self) -> int:
        return self.target_cursor.target_venue_ms

    @property
    def target_local_cursor_ms(self) -> int:
        return self.target_cursor.target_local_cursor_ms


@dataclass(frozen=True, slots=True)
class PaperFokEntryDecisionV2:
    attempt_id: str
    signal_event_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    lineage_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    target_cursor: CausalTargetCursorV2
    target_state_last_ingest_seq: int
    side: PaperFokSideV2
    requested_quantity: Decimal
    status: PaperFokEntryStatusV2
    inconclusive_cause: PaperFokInconclusiveCauseV2 | None
    closure_method: PaperFokClosureMethodV2
    certified_quantity: Decimal | None
    filled_quantity: Decimal | None
    executable_vwap: Decimal | None
    executable_notional: Decimal | None
    opposite_bbo: Decimal | None
    paper_price_cap: Decimal | None
    market_take_bound_price: Decimal | None
    evidence_sha256: str
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object]
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    role: str = field(init=False, default="PAPER_FOK_ENTRY")
    rule_version: str = field(init=False, default=PAPER_FOK_RULE_VERSION_V2)
    primary_depth_haircut: Decimal = field(
        init=False,
        default=PRIMARY_DEPTH_HAIRCUT_V2,
    )
    price_cap_rate: Decimal = field(init=False, default=PAPER_PRICE_CAP_RATE_V2)
    partial_primary_entry: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    discord_timing_present: bool = field(init=False, default=False)
    discord_timing_can_change_result: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise PaperFokContractErrorV2(
                "PAPER decision can be constructed only by the causal evaluator"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.signal_event_id, "signal_event_id")
        _validate_symbol_shape(self.symbol, "symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise PaperFokContractErrorV2("PAPER decision must remain USD-M only")
        for value, field_name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.source_root_sha256, "source_root_sha256"),
            (self.lineage_sha256, "lineage_sha256"),
            (self.evidence_sha256, "evidence_sha256"),
        ):
            _validate_sha256(value, field_name)
        try:
            validate_decision_bar_v2(
                self.bar_open_ms,
                self.bar_close_ms,
                self.decision_cutoff_ms,
            )
        except ValueError as exc:
            raise PaperFokContractErrorV2(str(exc)) from exc
        if not isinstance(self.target_cursor, CausalTargetCursorV2):
            raise PaperFokContractErrorV2("decision target_cursor must be CausalTargetCursorV2")
        if self.target_cursor.decision_cutoff_ms != self.decision_cutoff_ms:
            raise PaperFokContractErrorV2(
                "decision target cursor cutoff differs from the decision cutoff"
            )
        _validate_nonnegative_int(
            self.target_state_last_ingest_seq,
            "target_state_last_ingest_seq",
        )
        if not isinstance(self.side, PaperFokSideV2):
            raise PaperFokContractErrorV2("decision side must be BUY or SELL")
        _validate_decimal(
            self.requested_quantity,
            "requested_quantity",
            allow_zero=False,
        )
        if not isinstance(self.status, PaperFokEntryStatusV2):
            raise PaperFokContractErrorV2("status has the wrong type")
        if self.inconclusive_cause is not None and not isinstance(
            self.inconclusive_cause,
            PaperFokInconclusiveCauseV2,
        ):
            raise PaperFokContractErrorV2("inconclusive_cause has the wrong type")
        if self.status in (
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
        ):
            if self.inconclusive_cause is None:
                raise PaperFokContractErrorV2(
                    "inconclusive terminal decisions require a typed cause"
                )
            if (
                self.status is PaperFokEntryStatusV2.INCONCLUSIVE_FILTER
                and self.inconclusive_cause is not PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER
            ):
                raise PaperFokContractErrorV2("INCONCLUSIVE_FILTER requires its exact typed cause")
            if (
                self.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
                and self.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER
            ):
                raise PaperFokContractErrorV2("INCONCLUSIVE_DATA cannot carry a filter cause")
        elif self.inconclusive_cause is not None:
            raise PaperFokContractErrorV2(
                "non-inconclusive decisions cannot expose an inconclusive cause"
            )
        if not isinstance(self.closure_method, PaperFokClosureMethodV2):
            raise PaperFokContractErrorV2("closure_method has the wrong type")
        _validate_decision_quantities(self)
        _validate_reasons(self.reasons)
        _validate_identity(self.invalidation, "invalidation")
        identity = _decision_identity_document(self)
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(_EVENT_ID_DOMAIN + canonical_json_line(identity)).hexdigest(),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _PAYLOAD_DOMAIN
                + canonical_json_line(_decision_document(self, include_payload_sha256=False))
            ).hexdigest(),
        )

    @property
    def executed_full_quantity(self) -> bool:
        return self.status is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY

    @property
    def target_venue_ms(self) -> int:
        return self.target_cursor.target_venue_ms

    @property
    def target_local_cursor_ms(self) -> int:
        return self.target_cursor.target_local_cursor_ms


@dataclass(frozen=True, slots=True)
class PaperFokRegistryCheckpointV2:
    attempt_id: str
    promoting_plan_sha256: str
    replay_root_sha256: str
    event_count: int
    maximum_events: int
    checkpoint_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(self.replay_root_sha256, "replay_root_sha256")
        _validate_nonnegative_int(self.event_count, "event_count")
        if type(self.maximum_events) is not int or self.maximum_events < 1:
            raise PaperFokContractErrorV2("maximum_events must be positive")
        if self.event_count > self.maximum_events:
            raise PaperFokContractErrorV2("checkpoint event count exceeds capacity")
        object.__setattr__(
            self,
            "checkpoint_sha256",
            hashlib.sha256(
                _REGISTRY_CHECKPOINT_DOMAIN
                + canonical_json_line(_registry_checkpoint_document(self))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class PaperFokFullFillCertificateV2:
    attempt_id: str
    signal_event_id: str
    decision_event_id: str
    decision_payload_sha256: str
    evidence_sha256: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    target_cursor: CausalTargetCursorV2
    side: PaperFokSideV2
    requested_quantity: Decimal
    filled_quantity: Decimal
    executable_vwap: Decimal
    executable_notional: Decimal
    terminal_registry_replay_root_sha256: str
    terminal_registry_event_count: int
    terminal_registry_maximum_events: int
    terminal_registry_checkpoint_sha256: str
    certificate_sha256: str = field(init=False)
    _factory_token: InitVar[object]

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _CERTIFICATE_FACTORY_TOKEN:
            raise PaperFokContractErrorV2(
                "full-fill certificate can be issued only by the registry verifier"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        for value, field_name in (
            (self.signal_event_id, "signal_event_id"),
            (self.decision_event_id, "decision_event_id"),
            (self.decision_payload_sha256, "decision_payload_sha256"),
            (self.evidence_sha256, "evidence_sha256"),
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (
                self.terminal_registry_replay_root_sha256,
                "terminal_registry_replay_root_sha256",
            ),
            (
                self.terminal_registry_checkpoint_sha256,
                "terminal_registry_checkpoint_sha256",
            ),
        ):
            _validate_sha256(value, field_name)
        _validate_symbol_shape(self.symbol, "symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise PaperFokContractErrorV2("certificate venue must be USD-M Futures")
        if not isinstance(self.target_cursor, CausalTargetCursorV2):
            raise PaperFokContractErrorV2("certificate target_cursor must be CausalTargetCursorV2")
        if not isinstance(self.side, PaperFokSideV2):
            raise PaperFokContractErrorV2("certificate side must be BUY or SELL")
        _validate_decimal(
            self.requested_quantity,
            "requested_quantity",
            allow_zero=False,
        )
        _validate_decimal(self.filled_quantity, "filled_quantity", allow_zero=False)
        if self.requested_quantity != self.filled_quantity:
            raise PaperFokContractErrorV2(
                "certificate requested and filled quantities must be equal"
            )
        _validate_decimal(self.executable_vwap, "executable_vwap", allow_zero=False)
        _validate_decimal(
            self.executable_notional,
            "executable_notional",
            allow_zero=False,
        )
        _validate_nonnegative_int(
            self.terminal_registry_event_count,
            "terminal_registry_event_count",
        )
        if (
            type(self.terminal_registry_maximum_events) is not int
            or self.terminal_registry_maximum_events < 1
            or self.terminal_registry_event_count > self.terminal_registry_maximum_events
        ):
            raise PaperFokContractErrorV2("certificate registry count/capacity is invalid")
        object.__setattr__(
            self,
            "certificate_sha256",
            hashlib.sha256(
                _CERTIFICATE_DOMAIN + canonical_json_line(_certificate_document(self))
            ).hexdigest(),
        )

    @property
    def decision_cutoff_ms(self) -> int:
        return self.target_cursor.decision_cutoff_ms

    @property
    def target_venue_ms(self) -> int:
        return self.target_cursor.target_venue_ms


def issue_paper_fok_full_fill_certificate_v2(
    decision: PaperFokEntryDecisionV2,
    *,
    registry: PaperFokDecisionRegistryV2,
    externally_pinned_checkpoint_sha256: str,
) -> PaperFokFullFillCertificateV2:
    """Issue a registry-membership-bound proof of a full PAPER entry."""

    if not isinstance(decision, PaperFokEntryDecisionV2):
        raise PaperFokContractErrorV2("decision has the wrong type")
    if not isinstance(registry, PaperFokDecisionRegistryV2):
        raise PaperFokContractErrorV2("registry has the wrong type")
    _validate_sha256(
        externally_pinned_checkpoint_sha256,
        "externally_pinned_checkpoint_sha256",
    )
    if not decision.executed_full_quantity:
        raise PaperFokContractErrorV2("only full PAPER fills receive a certificate")
    canonical_paper_fok_entry_decision_v2(decision)
    if not registry.contains_exact_v2(decision):
        raise PaperFokContractErrorV2("full-fill decision is absent from the terminal registry")
    checkpoint = registry.terminal_checkpoint_v2()
    if checkpoint.checkpoint_sha256 != externally_pinned_checkpoint_sha256:
        raise PaperFokContractErrorV2("terminal registry checkpoint differs from the external pin")
    assert decision.filled_quantity is not None
    assert decision.executable_vwap is not None
    assert decision.executable_notional is not None
    certificate = PaperFokFullFillCertificateV2(
        attempt_id=decision.attempt_id,
        signal_event_id=decision.signal_event_id,
        decision_event_id=decision.event_id,
        decision_payload_sha256=decision.payload_sha256,
        evidence_sha256=decision.evidence_sha256,
        symbol=decision.symbol,
        venue=decision.venue,
        promoting_plan_sha256=decision.promoting_plan_sha256,
        target_cursor=decision.target_cursor,
        side=decision.side,
        requested_quantity=decision.requested_quantity,
        filled_quantity=decision.filled_quantity,
        executable_vwap=decision.executable_vwap,
        executable_notional=decision.executable_notional,
        terminal_registry_replay_root_sha256=checkpoint.replay_root_sha256,
        terminal_registry_event_count=checkpoint.event_count,
        terminal_registry_maximum_events=checkpoint.maximum_events,
        terminal_registry_checkpoint_sha256=checkpoint.checkpoint_sha256,
        _factory_token=_CERTIFICATE_FACTORY_TOKEN,
    )
    verify_paper_fok_full_fill_certificate_v2(
        certificate,
        decision,
        registry=registry,
        expected_attempt_id=decision.attempt_id,
        expected_promoting_plan_sha256=decision.promoting_plan_sha256,
        expected_target_cursor_evidence_sha256=(decision.target_cursor.cursor_evidence_sha256),
        expected_terminal_registry_checkpoint_sha256=(externally_pinned_checkpoint_sha256),
    )
    return certificate


def verify_paper_fok_full_fill_certificate_v2(
    certificate: PaperFokFullFillCertificateV2,
    decision: PaperFokEntryDecisionV2,
    *,
    registry: PaperFokDecisionRegistryV2,
    expected_attempt_id: str,
    expected_promoting_plan_sha256: str,
    expected_target_cursor_evidence_sha256: str,
    expected_terminal_registry_checkpoint_sha256: str,
) -> None:
    """Verify full-fill semantics without trusting certificate construction."""

    if not isinstance(certificate, PaperFokFullFillCertificateV2):
        raise PaperFokContractErrorV2("certificate has the wrong type")
    if not isinstance(decision, PaperFokEntryDecisionV2):
        raise PaperFokContractErrorV2("decision has the wrong type")
    if not isinstance(registry, PaperFokDecisionRegistryV2):
        raise PaperFokContractErrorV2("registry has the wrong type")
    _validate_identity(expected_attempt_id, "expected_attempt_id")
    for value, field_name in (
        (expected_promoting_plan_sha256, "expected_promoting_plan_sha256"),
        (
            expected_target_cursor_evidence_sha256,
            "expected_target_cursor_evidence_sha256",
        ),
        (
            expected_terminal_registry_checkpoint_sha256,
            "expected_terminal_registry_checkpoint_sha256",
        ),
    ):
        _validate_sha256(value, field_name)
    canonical_paper_fok_entry_decision_v2(decision)
    if not decision.executed_full_quantity:
        raise PaperFokContractErrorV2("verified decision is not a full PAPER fill")
    checkpoint = registry.terminal_checkpoint_v2()
    if not registry.contains_exact_v2(decision):
        raise PaperFokContractErrorV2("decision is absent from the pinned registry")
    expected_fields = (
        certificate.attempt_id == expected_attempt_id == decision.attempt_id,
        certificate.promoting_plan_sha256
        == expected_promoting_plan_sha256
        == decision.promoting_plan_sha256,
        certificate.signal_event_id == decision.signal_event_id,
        certificate.decision_event_id == decision.event_id,
        certificate.decision_payload_sha256 == decision.payload_sha256,
        certificate.evidence_sha256 == decision.evidence_sha256,
        certificate.symbol == decision.symbol,
        certificate.venue is decision.venue,
        certificate.target_cursor == decision.target_cursor,
        certificate.target_cursor.cursor_evidence_sha256 == expected_target_cursor_evidence_sha256,
        certificate.side is decision.side,
        certificate.requested_quantity == decision.requested_quantity,
        certificate.filled_quantity == certificate.requested_quantity == decision.filled_quantity,
        certificate.executable_vwap == decision.executable_vwap,
        certificate.executable_notional == decision.executable_notional,
        certificate.terminal_registry_replay_root_sha256 == checkpoint.replay_root_sha256,
        certificate.terminal_registry_event_count == checkpoint.event_count,
        certificate.terminal_registry_maximum_events == checkpoint.maximum_events,
        certificate.terminal_registry_checkpoint_sha256
        == expected_terminal_registry_checkpoint_sha256
        == checkpoint.checkpoint_sha256,
    )
    if not all(expected_fields):
        raise PaperFokContractErrorV2(
            "certificate, decision, target cursor, or registry pin differs"
        )
    expected_certificate_sha256 = hashlib.sha256(
        _CERTIFICATE_DOMAIN + canonical_json_line(_certificate_document(certificate))
    ).hexdigest()
    if certificate.certificate_sha256 != expected_certificate_sha256:
        raise PaperFokContractErrorV2("certificate hash differs from canonical content")


def canonical_paper_fok_full_fill_certificate_v2(
    certificate: PaperFokFullFillCertificateV2,
) -> bytes:
    """Validate and serialize a registry-issued full-fill certificate."""

    if not isinstance(certificate, PaperFokFullFillCertificateV2):
        raise PaperFokContractErrorV2("certificate has the wrong type")
    document = _certificate_document(certificate)
    expected = hashlib.sha256(_CERTIFICATE_DOMAIN + canonical_json_line(document)).hexdigest()
    if certificate.certificate_sha256 != expected:
        raise PaperFokContractErrorV2("certificate hash differs from canonical content")
    return canonical_json_line({**document, "certificate_sha256": certificate.certificate_sha256})


def multiply_protocol_decimals_exact_v2(left: Decimal, right: Decimal) -> Decimal:
    """Multiply two finite base-ten Decimals without ambient-context drift."""

    _validate_decimal(left, "left", allow_zero=True)
    _validate_decimal(right, "right", allow_zero=True)
    return _multiply_decimals_exact(left, right)


def decimal_fraction_v2(value: Decimal) -> Fraction:
    """Expose the exact rational value used by the shared execution owner."""

    _validate_decimal(value, "value", allow_zero=True)
    return _decimal_fraction(value)


def finite_base10_fraction_v2(value: Fraction) -> Decimal:
    """Convert an exact nonnegative finite-base-ten fraction to Decimal."""

    if not isinstance(value, Fraction):
        raise PaperFokContractErrorV2("value must be Fraction")
    return _base10_ratio_to_decimal(value.numerator, value.denominator)


def is_price_tick_aligned_v2(value: Decimal, tick_size: Decimal) -> bool:
    """Use the same exact tick test for entry and mandatory exit."""

    _validate_decimal(value, "value", allow_zero=False)
    _validate_decimal(tick_size, "tick_size", allow_zero=False)
    return _is_tick_aligned(value, tick_size)


@dataclass(frozen=True, slots=True)
class FuturesFrozenBookV2:
    """One causally reconstructed standard USD-M book generation.

    The type is public because entry capacity and mandatory post-entry exits
    must use the same snapshot/diff reconstruction owner.  It intentionally
    exposes no mutator and contains no RPI or bookTicker state.
    """

    bids: tuple[DepthLevelV2, ...]
    asks: tuple[DepthLevelV2, ...]
    prior_u: int
    bid_coverage_floor: Decimal | None
    ask_coverage_ceiling: Decimal | None
    bids_exhausted_at_snapshot: bool
    asks_exhausted_at_snapshot: bool


@dataclass(frozen=True, slots=True)
class _EvaluationV2:
    status: PaperFokEntryStatusV2
    inconclusive_cause: PaperFokInconclusiveCauseV2 | None
    closure_method: PaperFokClosureMethodV2
    certified_quantity: Decimal | None
    filled_quantity: Decimal | None
    executable_vwap: Decimal | None
    executable_notional: Decimal | None
    opposite_bbo: Decimal | None
    paper_price_cap: Decimal | None
    market_take_bound_price: Decimal | None
    reasons: tuple[str, ...]
    invalidation: str


def evaluate_authorized_paper_fok_entry_v2(
    item: PaperFokEntryInputV2,
    *,
    current_target_authority: object,
) -> PaperFokEntryDecisionV2:
    """Evaluate only after consuming current signed-prefix cursor authority.

    The low-level evaluator remains useful for deterministic arithmetic tests.
    Runtime PAPER admission must cross this seam so a caller-constructed legacy
    ``CausalTargetCursorV2`` cannot establish authority by matching scalar shape.
    """

    from signalbot.r4b_v2.capture.causal_target_authority import (
        CurrentCausalTargetAuthorityUseV2,
        consume_current_causal_target_authority_v2,
    )

    if type(item) is not PaperFokEntryInputV2:
        raise PaperFokContractErrorV2("item must be an exact PaperFokEntryInputV2")
    if type(current_target_authority) is not CurrentCausalTargetAuthorityUseV2:
        raise PaperFokContractErrorV2(
            "runtime PAPER evaluation requires current causal-target authority; "
            "direct CausalTargetCursorV2 values are rejected"
        )
    expected_plan_sha256 = current_target_authority.promoting_plan_sha256
    authorized_cursor = consume_current_causal_target_authority_v2(current_target_authority)
    if item.target_cursor != authorized_cursor:
        raise PaperFokContractErrorV2(
            "PAPER input cursor differs from current signed-prefix authority"
        )
    if item.lineage.promoting_plan_sha256 != expected_plan_sha256:
        raise PaperFokContractErrorV2(
            "PAPER lineage plan differs from current signed-prefix authority"
        )
    return evaluate_paper_fok_entry_v2(item)


def evaluate_paper_fok_entry_v2(
    item: PaperFokEntryInputV2,
) -> PaperFokEntryDecisionV2:
    """Evaluate one public-data-only PAPER FOK capacity qualifier.

    This pure arithmetic function performs no network call, order placement,
    portfolio mutation, fee booking, or PnL calculation. Runtime authority is
    deliberately outside this function and must use
    ``evaluate_authorized_paper_fok_entry_v2``. Discord state is absent from its
    input and therefore cannot alter admission, quantity, price, identity, or
    payload.
    """

    if not isinstance(item, PaperFokEntryInputV2):
        raise PaperFokContractErrorV2("item must be PaperFokEntryInputV2")
    evaluation = _evaluate_paper_fok(item)
    evidence_sha256 = hashlib.sha256(
        _EVIDENCE_DOMAIN + canonical_json_line(_evidence_document(item, evaluation=evaluation))
    ).hexdigest()
    return PaperFokEntryDecisionV2(
        attempt_id=item.attempt_id,
        signal_event_id=item.signal_event_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.lineage.promoting_plan_sha256,
        source_root_sha256=item.lineage.source_root_sha256,
        lineage_sha256=item.lineage.lineage_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        target_cursor=item.target_cursor,
        target_state_last_ingest_seq=item.target_state_last_ingest_seq,
        side=item.side,
        requested_quantity=item.requested_quantity,
        status=evaluation.status,
        inconclusive_cause=evaluation.inconclusive_cause,
        closure_method=evaluation.closure_method,
        certified_quantity=evaluation.certified_quantity,
        filled_quantity=evaluation.filled_quantity,
        executable_vwap=evaluation.executable_vwap,
        executable_notional=evaluation.executable_notional,
        opposite_bbo=evaluation.opposite_bbo,
        paper_price_cap=evaluation.paper_price_cap,
        market_take_bound_price=evaluation.market_take_bound_price,
        evidence_sha256=evidence_sha256,
        reasons=evaluation.reasons,
        invalidation=evaluation.invalidation,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _evaluate_paper_fok(item: PaperFokEntryInputV2) -> _EvaluationV2:
    provenance_reason = _validate_all_provenance(item)
    if provenance_reason is not None:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            provenance_reason,
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA,
        )
    book, book_reason = _reconstruct_target_book(item)
    if book is None:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            book_reason,
            cause=_book_failure_cause(book_reason),
        )
    closure_method, closure_reason = _classify_closure(item, prior_u=book.prior_u)
    if closure_method is PaperFokClosureMethodV2.PENDING:
        return _EvaluationV2(
            status=PaperFokEntryStatusV2.CLOSURE_PENDING,
            inconclusive_cause=None,
            closure_method=closure_method,
            certified_quantity=None,
            filled_quantity=None,
            executable_vwap=None,
            executable_notional=None,
            opposite_bbo=None,
            paper_price_cap=None,
            market_take_bound_price=None,
            reasons=(closure_reason,),
            invalidation="AWAIT_SEQUENCE_OR_QUIET_REST_CLOSURE",
        )
    if closure_method is PaperFokClosureMethodV2.INVALID:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            closure_reason,
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_CLOSURE,
            closure_method=closure_method,
        )
    mark_reason = _validate_mark_causality(item)
    if mark_reason is not None:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            mark_reason,
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_EXECUTION_RULE,
            closure_method=closure_method,
        )
    filter_reason = _validate_exchange_info_causality(item)
    if filter_reason is not None:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
            filter_reason,
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER,
            closure_method=closure_method,
        )
    try:
        grid = intersect_quantity_filters_v2(
            item.exchange_info.lot_size,
            item.exchange_info.market_lot_size,
        )
    except PaperFokContractErrorV2 as exc:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
            f"INCONCLUSIVE_FILTER_GRID:{exc}",
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER,
            closure_method=closure_method,
        )
    if not grid.is_legal(item.requested_quantity):
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
            "REQUESTED_QUANTITY_NOT_ON_FILTER_INTERSECTION",
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER,
            closure_method=closure_method,
        )
    return _walk_frozen_book(
        item,
        book=book,
        grid=grid,
        closure_method=closure_method,
    )


def _inconclusive(
    status: PaperFokEntryStatusV2,
    reason: str,
    *,
    cause: PaperFokInconclusiveCauseV2,
    closure_method: PaperFokClosureMethodV2 = PaperFokClosureMethodV2.INVALID,
) -> _EvaluationV2:
    return _EvaluationV2(
        status=status,
        inconclusive_cause=cause,
        closure_method=closure_method,
        certified_quantity=None,
        filled_quantity=None,
        executable_vwap=None,
        executable_notional=None,
        opposite_bbo=None,
        paper_price_cap=None,
        market_take_bound_price=None,
        reasons=(reason,),
        invalidation=reason,
    )


def _book_failure_cause(reason: str) -> PaperFokInconclusiveCauseV2:
    if any(token in reason for token in ("SEQUENCE", "INGEST", "BRIDGE", "PU_", "DEPTH_ROW")):
        return PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SEQUENCE
    return PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_BOOK


def _validate_all_provenance(item: PaperFokEntryInputV2) -> str | None:
    expected = item.lineage
    rows: list[tuple[object, str]] = [
        (item.snapshot, expected.depth_snapshot_schema_sha256),
        (item.mark, expected.mark_schema_sha256),
        (item.exchange_info, expected.exchange_info_schema_sha256),
    ]
    rows.extend(
        (event, expected.standard_depth_schema_sha256) for event in item.pre_target_depth_events
    )
    successor_material, _ = _first_successor_material(item.closure)
    rows.extend(
        (successor, expected.standard_depth_schema_sha256) for successor in successor_material
    )
    if item.closure.quiet_rest_snapshot is not None:
        rows.append(
            (
                item.closure.quiet_rest_snapshot,
                expected.depth_snapshot_schema_sha256,
            )
        )
    if item.closure.continuous_health is not None:
        rows.append((item.closure.continuous_health, expected.health_schema_sha256))
    for row, expected_schema in rows:
        if (
            getattr(row, "symbol", None) != item.symbol
            or getattr(row, "venue", None) is not item.venue
            or getattr(row, "promoting_plan_sha256", None) != expected.promoting_plan_sha256
            or getattr(row, "source_root_sha256", None) != expected.source_root_sha256
            or getattr(row, "schema_sha256", None) != expected_schema
        ):
            return "ROW_SYMBOL_VENUE_PLAN_SOURCE_OR_SCHEMA_ROOT_MISMATCH"
    routed_rows = [*item.pre_target_depth_events, item.mark]
    routed_rows.extend(successor_material)
    for row in routed_rows:
        if getattr(row, "routing_status", None) != 1:
            return "USD_M_ROUTING_STATUS_NOT_ONE"
        if getattr(row, "pair", None) != item.symbol:
            return "USD_M_ROUTING_PAIR_MISMATCH"
    return None


def _first_successor_material(
    closure: PaperFokClosureEvidenceV2,
) -> tuple[tuple[FuturesDepthContinuityWitnessV2, ...], bool]:
    """Return the canonical first-ingest evidence bucket and conflict flag.

    Exact duplicate capture rows collapse idempotently.  Distinct rows sharing
    the first ingest cursor are retained in canonical byte order so the conflict
    is deterministic.  Every later ingest is immaterial once that bucket exists.
    """

    if not closure.successor_candidates:
        return (), False
    first_ingest = min(value.ingest_seq for value in closure.successor_candidates)
    by_document: dict[bytes, FuturesDepthContinuityWitnessV2] = {}
    for value in closure.successor_candidates:
        if value.ingest_seq == first_ingest:
            by_document.setdefault(canonical_json_line(_witness_document(value)), value)
    material = tuple(by_document[key] for key in sorted(by_document))
    return material, len(material) > 1


def _normalize_pre_target_events(
    values: tuple[FuturesStandardDepthEventV2, ...],
) -> tuple[tuple[FuturesStandardDepthEventV2, ...], str | None]:
    """Collapse exact capture duplicates and reject same-cursor contradictions."""

    grouped: dict[int, dict[bytes, FuturesStandardDepthEventV2]] = {}
    for value in values:
        document = canonical_json_line(_depth_event_document(value))
        grouped.setdefault(value.ingest_seq, {}).setdefault(document, value)
    normalized: list[FuturesStandardDepthEventV2] = []
    for ingest_seq in sorted(grouped):
        distinct = grouped[ingest_seq]
        normalized.extend(distinct[key] for key in sorted(distinct))
        if len(distinct) > 1:
            return tuple(normalized), "CONFLICTING_DEPTH_ROWS_AT_SAME_INGEST_SEQUENCE"
    return tuple(normalized), None


def _reconstruct_target_book(
    item: PaperFokEntryInputV2,
) -> tuple[FuturesFrozenBookV2 | None, str]:
    return reconstruct_futures_standard_book_v2(
        snapshot=item.snapshot,
        pre_target_depth_events=item.pre_target_depth_events,
        target_venue_ms=item.target_venue_ms,
        target_local_cursor_ms=item.target_local_cursor_ms,
        target_state_last_ingest_seq=item.target_state_last_ingest_seq,
    )


def reconstruct_futures_standard_book_v2(
    *,
    snapshot: FuturesDepthSnapshotV2,
    pre_target_depth_events: tuple[FuturesStandardDepthEventV2, ...],
    target_venue_ms: int,
    target_local_cursor_ms: int,
    target_state_last_ingest_seq: int,
) -> tuple[FuturesFrozenBookV2 | None, str]:
    """Reconstruct one causal standard-depth generation for entry or exit.

    Callers remain responsible for checking symbol, plan, schema, and capture
    ledger membership.  This owner checks only deterministic book/sequence and
    target-causality semantics shared by both execution paths.
    """

    if not isinstance(snapshot, FuturesDepthSnapshotV2):
        raise PaperFokContractErrorV2("snapshot must be FuturesDepthSnapshotV2")
    if type(pre_target_depth_events) is not tuple or any(
        not isinstance(value, FuturesStandardDepthEventV2) for value in pre_target_depth_events
    ):
        raise PaperFokContractErrorV2(
            "pre_target_depth_events must be an immutable standard-depth tuple"
        )
    _validate_nonnegative_int(target_venue_ms, "target_venue_ms")
    _validate_nonnegative_int(target_local_cursor_ms, "target_local_cursor_ms")
    _validate_nonnegative_int(
        target_state_last_ingest_seq,
        "target_state_last_ingest_seq",
    )
    if snapshot.response_completion_ms > target_local_cursor_ms:
        return None, "DEPTH_SNAPSHOT_NOT_CAUSALLY_AVAILABLE_AT_TARGET"
    ordered_events, duplicate_reason = _normalize_pre_target_events(pre_target_depth_events)
    if duplicate_reason is not None:
        return None, duplicate_reason
    expected_last_ingest = ordered_events[-1].ingest_seq if ordered_events else 0
    if target_state_last_ingest_seq != expected_last_ingest:
        return None, "TARGET_STATE_LAST_INGEST_CURSOR_MISMATCH"
    prior_receipt = -1
    prior_transaction = -1
    prior_same_stream_ingest: int | None = None
    for event in ordered_events:
        if (
            event.transaction_time_ms > target_venue_ms
            or event.event_time_ms > target_venue_ms
            or event.receipt_completion_ms > target_local_cursor_ms
        ):
            return None, "NONCAUSAL_OR_POST_TARGET_DEPTH_ROW"
        if event.receipt_completion_ms < prior_receipt:
            return None, "DEPTH_RECEIPT_ORDER_CONTRADICTS_INGEST_ORDER"
        if event.transaction_time_ms < prior_transaction:
            return None, "DEPTH_TRANSACTION_ORDER_CONTRADICTS_INGEST_ORDER"
        if (
            prior_same_stream_ingest is not None
            and event.previous_same_stream_ingest_seq != prior_same_stream_ingest
        ):
            return None, "DEPTH_SAME_STREAM_INGEST_CHAIN_GAP"
        prior_receipt = event.receipt_completion_ms
        prior_transaction = event.transaction_time_ms
        prior_same_stream_ingest = event.ingest_seq

    bids = {level.price: level.quantity for level in snapshot.bids}
    asks = {level.price: level.quantity for level in snapshot.asks}
    snapshot_u = snapshot.last_update_id
    prior_u = snapshot_u
    bridged = False
    for event in ordered_events:
        if event.final_update_id < snapshot_u:
            continue
        if not bridged:
            if not (event.first_update_id <= snapshot_u <= event.final_update_id):
                return None, "FIRST_DEPTH_EVENT_DOES_NOT_BRIDGE_SNAPSHOT"
            bridged = True
        elif event.previous_final_update_id != prior_u:
            return None, "FUTURES_DEPTH_PU_SEQUENCE_GAP"
        elif event.final_update_id <= prior_u:
            return None, "POST_BRIDGE_DEPTH_U_DID_NOT_ADVANCE"
        _apply_level_updates(bids, event.bids)
        _apply_level_updates(asks, event.asks)
        prior_u = event.final_update_id
        if bids and asks and max(bids) >= min(asks):
            return None, "RECONSTRUCTED_BOOK_IS_CROSSED"

    if not bridged:
        return None, "DEPTH_SNAPSHOT_BRIDGE_MISSING"

    frozen_bids = tuple(
        DepthLevelV2(price=price, quantity=quantity)
        for price, quantity in sorted(bids.items(), reverse=True)
        if quantity > 0
    )
    frozen_asks = tuple(
        DepthLevelV2(price=price, quantity=quantity)
        for price, quantity in sorted(asks.items())
        if quantity > 0
    )
    if not frozen_bids or not frozen_asks:
        return None, "TARGET_BOOK_HAS_NO_TWO_SIDED_BBO"
    return (
        FuturesFrozenBookV2(
            bids=frozen_bids,
            asks=frozen_asks,
            prior_u=prior_u,
            bid_coverage_floor=(None if not snapshot.bids else snapshot.bids[-1].price),
            ask_coverage_ceiling=(None if not snapshot.asks else snapshot.asks[-1].price),
            bids_exhausted_at_snapshot=len(snapshot.bids) < snapshot.depth_limit,
            asks_exhausted_at_snapshot=len(snapshot.asks) < snapshot.depth_limit,
        ),
        "TARGET_BOOK_RECONSTRUCTED",
    )


def _apply_level_updates(
    book: dict[Decimal, Decimal],
    updates: tuple[DepthLevelV2, ...],
) -> None:
    for level in updates:
        if level.quantity == 0:
            book.pop(level.price, None)
        else:
            book[level.price] = level.quantity


def _classify_closure(
    item: PaperFokEntryInputV2,
    *,
    prior_u: int,
) -> tuple[PaperFokClosureMethodV2, str]:
    return classify_futures_book_closure_v2(
        closure=item.closure,
        target_local_cursor_ms=item.target_local_cursor_ms,
        target_state_last_ingest_seq=item.target_state_last_ingest_seq,
        prior_u=prior_u,
    )


def classify_futures_book_closure_v2(
    *,
    closure: PaperFokClosureEvidenceV2,
    target_local_cursor_ms: int,
    target_state_last_ingest_seq: int,
    prior_u: int,
) -> tuple[PaperFokClosureMethodV2, str]:
    """Classify shared post-target sequence or finalized quiet-book closure."""

    if not isinstance(closure, PaperFokClosureEvidenceV2):
        raise PaperFokContractErrorV2("closure must be PaperFokClosureEvidenceV2")
    _validate_nonnegative_int(target_local_cursor_ms, "target_local_cursor_ms")
    _validate_nonnegative_int(
        target_state_last_ingest_seq,
        "target_state_last_ingest_seq",
    )
    _validate_nonnegative_int(prior_u, "prior_u")
    if closure.closure_grace_end_local_ms <= target_local_cursor_ms:
        return PaperFokClosureMethodV2.INVALID, "CLOSURE_GRACE_MUST_FOLLOW_TARGET"
    material, conflicting_first = _first_successor_material(closure)
    if conflicting_first:
        return (
            PaperFokClosureMethodV2.INVALID,
            "CONFLICTING_SUCCESSOR_ROWS_AT_FIRST_INGEST_SEQUENCE",
        )
    if material:
        successor = material[0]
        if (
            successor.receipt_completion_ms <= target_local_cursor_ms
            or successor.receipt_completion_ms > closure.closure_grace_end_local_ms
            or successor.ingest_seq <= target_state_last_ingest_seq
        ):
            return (
                PaperFokClosureMethodV2.INVALID,
                "SUCCESSOR_IS_NOT_AFTER_SELECTED_TARGET_INGEST_CURSOR",
            )
        if successor.previous_same_stream_ingest_seq != target_state_last_ingest_seq:
            return (
                PaperFokClosureMethodV2.INVALID,
                "SUCCESSOR_OMITS_IMMEDIATE_SAME_STREAM_EVENT",
            )
        if successor.previous_final_update_id != prior_u:
            return PaperFokClosureMethodV2.INVALID, "SUCCESSOR_PU_DOES_NOT_EQUAL_PRIOR_U"
        if successor.final_update_id <= prior_u:
            return PaperFokClosureMethodV2.INVALID, "SUCCESSOR_U_DID_NOT_ADVANCE"
        return (
            PaperFokClosureMethodV2.CONTIGUOUS_SUCCESSOR,
            "CLOSED_BY_PRICE_FREE_CONTIGUOUS_SUCCESSOR",
        )
    if closure.finalized_through_local_ms < closure.closure_grace_end_local_ms:
        return PaperFokClosureMethodV2.PENDING, "CLOSURE_PENDING_UNTIL_FINALIZATION"
    quiet = closure.quiet_rest_snapshot
    health = closure.continuous_health
    if quiet is None or health is None:
        return PaperFokClosureMethodV2.INVALID, "QUIET_BOOK_FINAL_EVIDENCE_MISSING"
    if not (
        target_local_cursor_ms
        <= quiet.response_completion_ms
        <= closure.closure_grace_end_local_ms
        <= closure.finalized_through_local_ms
    ):
        return PaperFokClosureMethodV2.INVALID, "QUIET_REST_COMPLETION_OUTSIDE_GRACE"
    if not (
        health.interval_start_local_ms <= target_local_cursor_ms
        and health.interval_end_local_ms >= closure.closure_grace_end_local_ms
        and health.continuous
    ):
        return PaperFokClosureMethodV2.INVALID, "QUIET_BOOK_HEALTH_NOT_CONTINUOUS"
    if quiet.last_update_id != prior_u:
        return PaperFokClosureMethodV2.INVALID, "QUIET_REST_LAST_UPDATE_ID_MISMATCH"
    return PaperFokClosureMethodV2.QUIET_REST_EQUAL, "CLOSED_BY_QUIET_REST_EQUAL_PRIOR_U"


def _validate_mark_causality(item: PaperFokEntryInputV2) -> str | None:
    mark = item.mark
    if mark.event_time_ms > item.target_venue_ms:
        return "MARK_EVENT_TIME_AFTER_TARGET"
    if mark.receipt_completion_ms > item.target_local_cursor_ms:
        return "MARK_RECEIPT_AFTER_TARGET_CURSOR"
    if item.target_venue_ms - mark.event_time_ms > MARK_PRICE_MAX_STALENESS_MS_V2:
        return "MARK_PRICE_STALE_OVER_2000MS"
    return None


def _validate_exchange_info_causality(item: PaperFokEntryInputV2) -> str | None:
    rules = item.exchange_info
    if not rules.applicable_filter_inventory_complete:
        return "APPLICABLE_FILTER_INVENTORY_IS_INCOMPLETE"
    if rules.response_completion_ms > item.target_local_cursor_ms:
        return "EXCHANGE_INFO_RESPONSE_AFTER_TARGET_CURSOR"
    if not (
        rules.version_valid_from_local_ms
        <= item.target_local_cursor_ms
        <= rules.version_valid_through_local_ms
    ):
        return "EXCHANGE_INFO_VERSION_NOT_CERTAIN_AT_TARGET"
    return None


def _walk_frozen_book(
    item: PaperFokEntryInputV2,
    *,
    book: FuturesFrozenBookV2,
    grid: CommonQuantityGridV2,
    closure_method: PaperFokClosureMethodV2,
) -> _EvaluationV2:
    rules = item.exchange_info
    if item.side is PaperFokSideV2.BUY:
        levels = book.asks
        opposite_bbo = levels[0].price
        paper_cap = _floor_to_tick_exact(
            _multiply_decimals_exact(opposite_bbo, Decimal("1.0010")),
            rules.tick_size,
        )
        market_bound = _multiply_decimals_exact(
            item.mark.mark_price,
            _add_decimals_exact(Decimal(1), rules.market_take_bound),
        )
        coverage_complete = book.asks_exhausted_at_snapshot or (
            book.ask_coverage_ceiling is not None and book.ask_coverage_ceiling >= paper_cap
        )

        def paper_cap_allows(price: Decimal) -> bool:
            return price <= paper_cap

    else:
        levels = book.bids
        opposite_bbo = levels[0].price
        paper_cap = _ceil_to_tick_exact(
            _multiply_decimals_exact(opposite_bbo, Decimal("0.9990")),
            rules.tick_size,
        )
        market_bound = _multiply_decimals_exact(
            item.mark.mark_price,
            _subtract_decimals_exact(Decimal(1), rules.market_take_bound),
        )
        coverage_complete = book.bids_exhausted_at_snapshot or (
            book.bid_coverage_floor is not None and book.bid_coverage_floor <= paper_cap
        )

        def paper_cap_allows(price: Decimal) -> bool:
            return price >= paper_cap

    if not coverage_complete:
        return _inconclusive(
            PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
            "TEN_BP_BOOK_COVERAGE_IS_NOT_PROVEN",
            cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_BOOK,
            closure_method=closure_method,
        )

    percent_lower = (
        None
        if rules.percent_price_multiplier_down is None
        else _multiply_decimals_exact(
            item.mark.mark_price,
            rules.percent_price_multiplier_down,
        )
    )
    percent_upper = (
        None
        if rules.percent_price_multiplier_up is None
        else _multiply_decimals_exact(
            item.mark.mark_price,
            rules.percent_price_multiplier_up,
        )
    )
    requested_notional = _multiply_decimals_exact(
        item.mark.mark_price,
        item.requested_quantity,
    )
    if (rules.min_notional > 0 and requested_notional < rules.min_notional) or (
        rules.max_notional > 0 and requested_notional > rules.max_notional
    ):
        return _EvaluationV2(
            status=PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL,
            inconclusive_cause=None,
            closure_method=closure_method,
            certified_quantity=Decimal(0),
            filled_quantity=Decimal(0),
            executable_vwap=None,
            executable_notional=None,
            opposite_bbo=opposite_bbo,
            paper_price_cap=paper_cap,
            market_take_bound_price=market_bound,
            reasons=("CERTAIN_NOTIONAL_FILTER_REJECTION",),
            invalidation="REQUESTED_MARK_NOTIONAL_OUTSIDE_ENABLED_BOUNDS",
        )

    capacities: list[tuple[Decimal, int]] = []
    total_capacity_units = 0
    for level in levels:
        if not paper_cap_allows(level.price):
            break
        if not _is_tick_aligned(level.price, rules.tick_size):
            return _inconclusive(
                PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
                "CONSUMED_DEPTH_LEVEL_IS_OFF_PRICE_TICK",
                cause=PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER,
                closure_method=closure_method,
            )
        if not futures_level_passes_official_bounds_v2(
            item.side,
            level.price,
            rules=rules,
            market_bound=market_bound,
            percent_lower=percent_lower,
            percent_upper=percent_upper,
        ):
            break
        haircutted = _multiply_decimals_exact(
            level.quantity,
            Decimal("0.50"),
        )
        capacity = grid.floor_capacity_per_level(haircutted)
        capacity_units = _decimal_to_units_required(capacity, grid.scale)
        if capacity_units:
            capacities.append((level.price, capacity_units))
            total_capacity_units += capacity_units
    certified_quantity = _units_to_decimal(total_capacity_units, grid.scale)
    if certified_quantity == 0:
        return _EvaluationV2(
            status=PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL,
            inconclusive_cause=None,
            closure_method=closure_method,
            certified_quantity=Decimal(0),
            filled_quantity=Decimal(0),
            executable_vwap=None,
            executable_notional=None,
            opposite_bbo=opposite_bbo,
            paper_price_cap=paper_cap,
            market_take_bound_price=market_bound,
            reasons=("CERTIFIED_QUANTITY_EQUALS_ZERO",),
            invalidation="NO_PRIMARY_HAIRCUT_CAPACITY_AT_TARGET",
        )
    if certified_quantity < item.requested_quantity:
        return _EvaluationV2(
            status=PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY,
            inconclusive_cause=None,
            closure_method=closure_method,
            certified_quantity=certified_quantity,
            filled_quantity=Decimal(0),
            executable_vwap=None,
            executable_notional=None,
            opposite_bbo=opposite_bbo,
            paper_price_cap=paper_cap,
            market_take_bound_price=market_bound,
            reasons=("CERTIFIED_QUANTITY_BELOW_REQUESTED_QUANTITY",),
            invalidation="PAPER_FOK_FULL_QUANTITY_NOT_CERTIFIED",
        )
    requested_units = _decimal_to_units_required(item.requested_quantity, grid.scale)
    vwap, executable_notional, exact_notional = _full_quantity_vwap(
        capacities,
        requested_units=requested_units,
        scale=grid.scale,
    )
    if (rules.min_notional > 0 and exact_notional < _decimal_fraction(rules.min_notional)) or (
        rules.max_notional > 0 and exact_notional > _decimal_fraction(rules.max_notional)
    ):
        return _EvaluationV2(
            status=PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL,
            inconclusive_cause=None,
            closure_method=closure_method,
            certified_quantity=Decimal(0),
            filled_quantity=Decimal(0),
            executable_vwap=None,
            executable_notional=None,
            opposite_bbo=opposite_bbo,
            paper_price_cap=paper_cap,
            market_take_bound_price=market_bound,
            reasons=("CERTAIN_EXECUTABLE_NOTIONAL_FILTER_REJECTION",),
            invalidation="FULL_FILL_NOTIONAL_OUTSIDE_ENABLED_BOUNDS",
        )
    return _EvaluationV2(
        status=PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY,
        inconclusive_cause=None,
        closure_method=closure_method,
        certified_quantity=certified_quantity,
        filled_quantity=item.requested_quantity,
        executable_vwap=vwap,
        executable_notional=executable_notional,
        opposite_bbo=opposite_bbo,
        paper_price_cap=paper_cap,
        market_take_bound_price=market_bound,
        reasons=(
            "FULL_REQUESTED_QUANTITY_CERTIFIED",
            "PER_LEVEL_HAIRCUT_AND_ALL_PRICE_BOUNDS_APPLIED",
        ),
        invalidation="EXIT_UNDER_FROZEN_FAMILY_RULE_OR_MANDATORY_EMERGENCY",
    )


def _full_quantity_vwap(
    capacities: list[tuple[Decimal, int]],
    *,
    requested_units: int,
    scale: int,
) -> tuple[Decimal, Decimal, Fraction]:
    remaining = requested_units
    notional = Fraction(0, 1)
    for price, capacity_units in capacities:
        taken = min(remaining, capacity_units)
        if taken:
            notional += _decimal_fraction(price) * Fraction(taken, scale)
            remaining -= taken
        if remaining == 0:
            break
    if remaining:
        raise PaperFokContractErrorV2(
            "internal certified capacity cannot satisfy requested quantity"
        )
    quantity = Fraction(requested_units, scale)
    vwap = notional / quantity
    with localcontext(protocol_decimal_context_v2()):
        vwap_decimal = Decimal(vwap.numerator) / Decimal(vwap.denominator)
    executable_notional = _base10_ratio_to_decimal(
        notional.numerator,
        notional.denominator,
    )
    return vwap_decimal, executable_notional, notional


def _multiply_decimals_exact(left: Decimal, right: Decimal) -> Decimal:
    left_numerator, left_denominator = _decimal_ratio(left)
    right_numerator, right_denominator = _decimal_ratio(right)
    return _base10_ratio_to_decimal(
        left_numerator * right_numerator,
        left_denominator * right_denominator,
    )


def _add_decimals_exact(left: Decimal, right: Decimal) -> Decimal:
    left_numerator, left_denominator = _decimal_ratio(left)
    right_numerator, right_denominator = _decimal_ratio(right)
    return _base10_ratio_to_decimal(
        left_numerator * right_denominator + right_numerator * left_denominator,
        left_denominator * right_denominator,
    )


def _subtract_decimals_exact(left: Decimal, right: Decimal) -> Decimal:
    left_numerator, left_denominator = _decimal_ratio(left)
    right_numerator, right_denominator = _decimal_ratio(right)
    numerator = left_numerator * right_denominator - right_numerator * left_denominator
    if numerator < 0:
        raise PaperFokContractErrorV2("Decimal subtraction produced a negative value")
    return _base10_ratio_to_decimal(
        numerator,
        left_denominator * right_denominator,
    )


def _floor_to_tick_exact(value: Decimal, tick_size: Decimal) -> Decimal:
    value_numerator, value_denominator = _decimal_ratio(value)
    tick_numerator, tick_denominator = _decimal_ratio(tick_size)
    ticks = value_numerator * tick_denominator // (value_denominator * tick_numerator)
    return _base10_ratio_to_decimal(
        ticks * tick_numerator,
        tick_denominator,
    )


def _ceil_to_tick_exact(value: Decimal, tick_size: Decimal) -> Decimal:
    value_numerator, value_denominator = _decimal_ratio(value)
    tick_numerator, tick_denominator = _decimal_ratio(tick_size)
    numerator = value_numerator * tick_denominator
    denominator = value_denominator * tick_numerator
    ticks = _ceil_div(numerator, denominator)
    return _base10_ratio_to_decimal(
        ticks * tick_numerator,
        tick_denominator,
    )


def _is_tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
    value_numerator, value_denominator = _decimal_ratio(value)
    tick_numerator, tick_denominator = _decimal_ratio(tick_size)
    return value_numerator * tick_denominator % (value_denominator * tick_numerator) == 0


def futures_level_passes_official_bounds_v2(
    side: PaperFokSideV2,
    price: Decimal,
    *,
    rules: FuturesExchangeInfoEvidenceV2,
    market_bound: Decimal,
    percent_lower: Decimal | None,
    percent_upper: Decimal | None,
) -> bool:
    """Return whether a captured USD-M level satisfies frozen public bounds."""

    if rules.min_price > 0 and price < rules.min_price:
        return False
    if rules.max_price > 0 and price > rules.max_price:
        return False
    if percent_lower is not None and price < percent_lower:
        return False
    if percent_upper is not None and price > percent_upper:
        return False
    if side is PaperFokSideV2.BUY:
        return price <= market_bound
    return price >= market_bound


def _base10_ratio_to_decimal(numerator: int, denominator: int) -> Decimal:
    if type(numerator) is not int or numerator < 0:
        raise PaperFokContractErrorV2("ratio numerator must be nonnegative")
    if type(denominator) is not int or denominator < 1:
        raise PaperFokContractErrorV2("ratio denominator must be positive")
    twos = 0
    fives = 0
    remainder = denominator
    while remainder % 2 == 0:
        twos += 1
        remainder //= 2
    while remainder % 5 == 0:
        fives += 1
        remainder //= 5
    if remainder != 1:
        raise PaperFokContractErrorV2("ratio is not a finite base-10 Decimal")
    places = max(twos, fives)
    numerator *= 2 ** (places - twos) * 5 ** (places - fives)
    exponent = -places
    digits = tuple(int(character) for character in str(numerator))
    return Decimal((0, digits, exponent))


def _decimal_fraction(value: Decimal) -> Fraction:
    numerator, denominator = _decimal_ratio(value)
    return Fraction(numerator, denominator)


def canonical_paper_fok_entry_decision_v2(
    decision: PaperFokEntryDecisionV2,
) -> bytes:
    """Return the canonical, evaluator-sealed PAPER entry ledger row."""

    if not isinstance(decision, PaperFokEntryDecisionV2):
        raise PaperFokContractErrorV2("decision must be PaperFokEntryDecisionV2")
    expected = hashlib.sha256(
        _PAYLOAD_DOMAIN
        + canonical_json_line(_decision_document(decision, include_payload_sha256=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise PaperFokContractErrorV2("PAPER decision payload hash differs from canonical content")
    return canonical_json_line(_decision_document(decision, include_payload_sha256=True))


class PaperFokDecisionRegistryV2:
    """Bounded idempotency/conflict gate with deterministic replay state."""

    def __init__(
        self,
        *,
        maximum_events: int,
        attempt_id: str,
        promoting_plan_sha256: str,
    ) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise PaperFokContractErrorV2("maximum_events must be positive")
        _validate_identity(attempt_id, "attempt_id")
        _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
        self._maximum_events = maximum_events
        self._attempt_id = attempt_id
        self._promoting_plan_sha256 = promoting_plan_sha256
        self._payload_by_event_id: dict[str, bytes] = {}
        self._order_key_by_event_id: dict[str, tuple[int, str, str]] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def attempt_id(self) -> str:
        return self._attempt_id

    @property
    def promoting_plan_sha256(self) -> str:
        return self._promoting_plan_sha256

    @property
    def replay_root_sha256(self) -> str:
        return _registry_replay_root(
            self._ordered_state_rows(),
            attempt_id=self._attempt_id,
            promoting_plan_sha256=self._promoting_plan_sha256,
            maximum_events=self._maximum_events,
        )

    def terminal_checkpoint_v2(self) -> PaperFokRegistryCheckpointV2:
        return PaperFokRegistryCheckpointV2(
            attempt_id=self._attempt_id,
            promoting_plan_sha256=self._promoting_plan_sha256,
            replay_root_sha256=self.replay_root_sha256,
            event_count=self.event_count,
            maximum_events=self._maximum_events,
        )

    def contains_exact_v2(self, decision: PaperFokEntryDecisionV2) -> bool:
        if not isinstance(decision, PaperFokEntryDecisionV2):
            raise PaperFokContractErrorV2("decision has the wrong type")
        payload = canonical_paper_fok_entry_decision_v2(decision)
        return self._payload_by_event_id.get(decision.event_id) == payload

    def register(
        self,
        decision: PaperFokEntryDecisionV2,
    ) -> PaperFokRegistryDispositionV2:
        if not isinstance(decision, PaperFokEntryDecisionV2):
            raise PaperFokContractErrorV2("decision has the wrong type")
        if (
            decision.attempt_id != self._attempt_id
            or decision.promoting_plan_sha256 != self._promoting_plan_sha256
        ):
            raise PaperFokContractErrorV2(
                "decision attempt or promoting plan differs from the registry"
            )
        if decision.status is PaperFokEntryStatusV2.CLOSURE_PENDING:
            raise PaperFokContractErrorV2(
                "CLOSURE_PENDING is a monitoring view, not a terminal registry row"
            )
        payload = canonical_paper_fok_entry_decision_v2(decision)
        order_key = (decision.target_venue_ms, decision.symbol, decision.event_id)
        prior = self._payload_by_event_id.get(decision.event_id)
        if prior is not None:
            if prior != payload:
                raise PaperFokContractErrorV2(
                    "deterministic PAPER event ID collides with different payload"
                )
            return PaperFokRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise PaperFokContractErrorV2("bounded PAPER decision registry capacity exhausted")
        self._payload_by_event_id[decision.event_id] = payload
        self._order_key_by_event_id[decision.event_id] = order_key
        return PaperFokRegistryDispositionV2.NEW

    def export_state_v2(self) -> bytes:
        rows = self._ordered_state_rows()
        checkpoint = self.terminal_checkpoint_v2()
        return canonical_json_line(
            {
                "attempt_id": self._attempt_id,
                "checkpoint_sha256": checkpoint.checkpoint_sha256,
                "event_count": len(rows),
                "events": rows,
                "maximum_events": self._maximum_events,
                "promoting_plan_sha256": self._promoting_plan_sha256,
                "replay_root_sha256": checkpoint.replay_root_sha256,
                "schema_version": _REGISTRY_STATE_SCHEMA,
            }
        )

    @classmethod
    def from_state_v2(
        cls,
        payload: bytes,
        *,
        expected_replay_root_sha256: str,
        expected_event_count: int,
        expected_maximum_events: int,
        expected_attempt_id: str,
        expected_promoting_plan_sha256: str,
        expected_checkpoint_sha256: str,
    ) -> PaperFokDecisionRegistryV2:
        for value, field_name in (
            (expected_replay_root_sha256, "expected_replay_root_sha256"),
            (
                expected_promoting_plan_sha256,
                "expected_promoting_plan_sha256",
            ),
            (expected_checkpoint_sha256, "expected_checkpoint_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_identity(expected_attempt_id, "expected_attempt_id")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        if type(expected_maximum_events) is not int or expected_maximum_events < 1:
            raise PaperFokContractErrorV2("expected_maximum_events must be positive")
        if expected_event_count > expected_maximum_events:
            raise PaperFokContractErrorV2("expected registry count exceeds expected capacity")
        if type(payload) is not bytes or not payload:
            raise PaperFokContractErrorV2("registry state must be non-empty bytes")
        try:
            document = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PaperFokContractErrorV2("registry state is invalid UTF-8 JSON") from exc
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise PaperFokContractErrorV2("registry state must be canonical JSONL")
        if (
            set(document)
            != {
                "attempt_id",
                "checkpoint_sha256",
                "event_count",
                "events",
                "maximum_events",
                "promoting_plan_sha256",
                "replay_root_sha256",
                "schema_version",
            }
            or document.get("schema_version") != _REGISTRY_STATE_SCHEMA
        ):
            raise PaperFokContractErrorV2("registry state schema is unsupported")
        maximum_events = document.get("maximum_events")
        if maximum_events != expected_maximum_events:
            raise PaperFokContractErrorV2("registry maximum_events differs from pin")
        if document.get("attempt_id") != expected_attempt_id:
            raise PaperFokContractErrorV2("registry attempt differs from pin")
        if document.get("promoting_plan_sha256") != expected_promoting_plan_sha256:
            raise PaperFokContractErrorV2("registry promoting plan differs from pin")
        if document.get("event_count") != expected_event_count:
            raise PaperFokContractErrorV2("registry event count differs from pin")
        raw_rows = document.get("events")
        if (
            not isinstance(raw_rows, list)
            or len(raw_rows) != expected_event_count
            or len(raw_rows) > expected_maximum_events
        ):
            raise PaperFokContractErrorV2("registry event rows exceed capacity")
        registry = cls(
            maximum_events=expected_maximum_events,
            attempt_id=expected_attempt_id,
            promoting_plan_sha256=expected_promoting_plan_sha256,
        )
        rows: list[dict[str, object]] = []
        prior_key: tuple[int, str, str] | None = None
        for raw_row in raw_rows:
            row, event_id, order_key, decision_payload = _parse_registry_row(
                raw_row,
                expected_attempt_id=expected_attempt_id,
                expected_promoting_plan_sha256=expected_promoting_plan_sha256,
            )
            if prior_key is not None and order_key <= prior_key:
                raise PaperFokContractErrorV2("registry rows are not in strict deterministic order")
            prior_key = order_key
            if event_id in registry._payload_by_event_id:
                raise PaperFokContractErrorV2("registry state repeats an event ID")
            registry._payload_by_event_id[event_id] = decision_payload
            registry._order_key_by_event_id[event_id] = order_key
            rows.append(row)
        observed_root = document.get("replay_root_sha256")
        _validate_sha256_value(observed_root, "replay_root_sha256")
        computed_root = _registry_replay_root(
            rows,
            attempt_id=expected_attempt_id,
            promoting_plan_sha256=expected_promoting_plan_sha256,
            maximum_events=expected_maximum_events,
        )
        if observed_root != computed_root or observed_root != expected_replay_root_sha256:
            raise PaperFokContractErrorV2("registry replay root mismatch")
        checkpoint = registry.terminal_checkpoint_v2()
        observed_checkpoint = document.get("checkpoint_sha256")
        _validate_sha256_value(observed_checkpoint, "checkpoint_sha256")
        if (
            observed_checkpoint != checkpoint.checkpoint_sha256
            or observed_checkpoint != expected_checkpoint_sha256
        ):
            raise PaperFokContractErrorV2("registry checkpoint mismatch")
        return registry

    def _ordered_state_rows(self) -> list[dict[str, object]]:
        rows = [
            _registry_state_row(
                event_id=event_id,
                order_key=self._order_key_by_event_id[event_id],
                payload=payload,
            )
            for event_id, payload in self._payload_by_event_id.items()
        ]
        rows.sort(key=_registry_row_sort_key)
        return rows


def _registry_state_row(
    *,
    event_id: str,
    order_key: tuple[int, str, str],
    payload: bytes,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "order_key": list(order_key),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _registry_row_sort_key(row: dict[str, object]) -> tuple[int, str, str]:
    raw_key = row["order_key"]
    if not isinstance(raw_key, list) or len(raw_key) != 3:
        raise PaperFokContractErrorV2("internal registry order key is malformed")
    target_venue_ms, symbol, event_id = raw_key
    if (
        type(target_venue_ms) is not int
        or not isinstance(symbol, str)
        or not isinstance(event_id, str)
    ):
        raise PaperFokContractErrorV2("internal registry order key types are invalid")
    return target_venue_ms, symbol, event_id


def _registry_replay_root(
    rows: list[dict[str, object]],
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    maximum_events: int,
) -> str:
    return hashlib.sha256(
        _REPLAY_ROOT_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": attempt_id,
                "event_count": len(rows),
                "events": rows,
                "maximum_events": maximum_events,
                "promoting_plan_sha256": promoting_plan_sha256,
                "schema_version": "r4b_paper_fok_registry_replay_v2",
            }
        )
    ).hexdigest()


def _registry_checkpoint_document(
    value: PaperFokRegistryCheckpointV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "event_count": value.event_count,
        "maximum_events": value.maximum_events,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "replay_root_sha256": value.replay_root_sha256,
        "schema_version": "r4b_paper_fok_registry_checkpoint_v2",
    }


def _parse_registry_row(
    raw_row: object,
    *,
    expected_attempt_id: str,
    expected_promoting_plan_sha256: str,
) -> tuple[dict[str, object], str, tuple[int, str, str], bytes]:
    if not isinstance(raw_row, dict) or set(raw_row) != {
        "event_id",
        "order_key",
        "payload_base64",
        "payload_sha256",
    }:
        raise PaperFokContractErrorV2("registry row schema is unsupported")
    event_id = raw_row.get("event_id")
    _validate_sha256_value(event_id, "event_id")
    assert isinstance(event_id, str)
    raw_key = raw_row.get("order_key")
    if not isinstance(raw_key, list) or len(raw_key) != 3:
        raise PaperFokContractErrorV2("registry row order key is invalid")
    target_venue_ms, symbol, key_event_id = raw_key
    if type(target_venue_ms) is not int or target_venue_ms < 0:
        raise PaperFokContractErrorV2("registry target time is invalid")
    if not isinstance(symbol, str):
        raise PaperFokContractErrorV2("registry symbol is invalid")
    _validate_symbol_shape(symbol, "symbol")
    if key_event_id != event_id:
        raise PaperFokContractErrorV2("registry key event ID differs from row")
    encoded = raw_row.get("payload_base64")
    if not isinstance(encoded, str):
        raise PaperFokContractErrorV2("registry payload_base64 must be text")
    try:
        decision_payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise PaperFokContractErrorV2("registry payload_base64 is invalid") from exc
    if base64.b64encode(decision_payload).decode("ascii") != encoded:
        raise PaperFokContractErrorV2("registry payload_base64 is noncanonical")
    payload_sha256 = raw_row.get("payload_sha256")
    _validate_sha256_value(payload_sha256, "payload_sha256")
    if payload_sha256 != hashlib.sha256(decision_payload).hexdigest():
        raise PaperFokContractErrorV2("registry row payload hash mismatch")
    order_key = (target_venue_ms, symbol, event_id)
    _validate_registry_decision_payload(
        decision_payload,
        event_id=event_id,
        order_key=order_key,
        expected_attempt_id=expected_attempt_id,
        expected_promoting_plan_sha256=expected_promoting_plan_sha256,
    )
    row = _registry_state_row(
        event_id=event_id,
        order_key=order_key,
        payload=decision_payload,
    )
    if row != raw_row:
        raise PaperFokContractErrorV2("registry row is not canonical")
    return row, event_id, order_key, decision_payload


def _validate_registry_decision_payload(
    payload: bytes,
    *,
    event_id: str,
    order_key: tuple[int, str, str],
    expected_attempt_id: str,
    expected_promoting_plan_sha256: str,
) -> None:
    try:
        document = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PaperFokContractErrorV2("registry decision payload is invalid JSON") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise PaperFokContractErrorV2("registry decision payload is not canonical")
    decision = _decision_from_document(document)
    target_venue_ms, symbol, key_event_id = order_key
    if (
        decision.event_id != event_id
        or key_event_id != event_id
        or decision.target_venue_ms != target_venue_ms
        or decision.symbol != symbol
        or decision.attempt_id != expected_attempt_id
        or decision.promoting_plan_sha256 != expected_promoting_plan_sha256
    ):
        raise PaperFokContractErrorV2(
            "registry decision identity, attempt, or promoting plan differs"
        )


def _decision_from_document(
    document: dict[str, object],
) -> PaperFokEntryDecisionV2:
    expected_keys = {
        "attempt_id",
        "bar_close_ms",
        "bar_open_ms",
        "certified_quantity",
        "closure_method",
        "decision_cutoff_ms",
        "discord_timing_can_change_result",
        "discord_timing_present",
        "event_id",
        "evidence_sha256",
        "executable_notional",
        "executable_vwap",
        "filled_quantity",
        "inconclusive_cause",
        "invalidation",
        "lineage_sha256",
        "market_take_bound_price",
        "opposite_bbo",
        "paper_price_cap",
        "partial_primary_entry",
        "payload_sha256",
        "price_cap_rate",
        "primary_depth_haircut",
        "production_order_placement",
        "promoting_plan_sha256",
        "reasons",
        "requested_quantity",
        "role",
        "rule_version",
        "side",
        "signal_event_id",
        "source_root_sha256",
        "status",
        "symbol",
        "target_cursor",
        "target_state_last_ingest_seq",
        "target_venue_ms",
        "venue",
    }
    if set(document) != expected_keys:
        raise PaperFokContractErrorV2("registry decision payload schema is not exact")
    if (
        document.get("venue") != VenueV2.USDM_FUTURES.value
        or document.get("role") != "PAPER_FOK_ENTRY"
        or document.get("rule_version") != PAPER_FOK_RULE_VERSION_V2
        or document.get("production_order_placement") is not False
        or document.get("discord_timing_present") is not False
        or document.get("discord_timing_can_change_result") is not False
        or document.get("partial_primary_entry") is not False
        or document.get("primary_depth_haircut") != str(PRIMARY_DEPTH_HAIRCUT_V2)
        or document.get("price_cap_rate") != str(PAPER_PRICE_CAP_RATE_V2)
    ):
        raise PaperFokContractErrorV2("registry decision frozen contract fields differ")
    text_fields = (
        "attempt_id",
        "signal_event_id",
        "symbol",
        "promoting_plan_sha256",
        "source_root_sha256",
        "lineage_sha256",
        "evidence_sha256",
        "invalidation",
    )
    if any(not isinstance(document.get(name), str) for name in text_fields):
        raise PaperFokContractErrorV2("registry decision text field is invalid")
    int_fields = (
        "bar_open_ms",
        "bar_close_ms",
        "decision_cutoff_ms",
        "target_state_last_ingest_seq",
        "target_venue_ms",
    )
    if any(type(document.get(name)) is not int for name in int_fields):
        raise PaperFokContractErrorV2("registry decision integer field is invalid")
    reasons = document.get("reasons")
    if not isinstance(reasons, list) or any(not isinstance(value, str) for value in reasons):
        raise PaperFokContractErrorV2("registry reasons are invalid")
    target_cursor = _target_cursor_from_document(document.get("target_cursor"))
    side_raw = document.get("side")
    status_raw = document.get("status")
    closure_raw = document.get("closure_method")
    cause_raw = document.get("inconclusive_cause")
    if (
        not isinstance(side_raw, str)
        or not isinstance(status_raw, str)
        or not isinstance(closure_raw, str)
        or (cause_raw is not None and not isinstance(cause_raw, str))
    ):
        raise PaperFokContractErrorV2("registry enum field is invalid")
    try:
        side = PaperFokSideV2(side_raw)
        status = PaperFokEntryStatusV2(status_raw)
        closure_method = PaperFokClosureMethodV2(closure_raw)
        cause = None if cause_raw is None else PaperFokInconclusiveCauseV2(cause_raw)
    except ValueError as exc:
        raise PaperFokContractErrorV2("registry enum value is invalid") from exc
    attempt_id = document["attempt_id"]
    signal_event_id = document["signal_event_id"]
    symbol = document["symbol"]
    promoting_plan_sha256 = document["promoting_plan_sha256"]
    source_root_sha256 = document["source_root_sha256"]
    lineage_sha256 = document["lineage_sha256"]
    evidence_sha256 = document["evidence_sha256"]
    invalidation = document["invalidation"]
    bar_open_ms = document["bar_open_ms"]
    bar_close_ms = document["bar_close_ms"]
    decision_cutoff_ms = document["decision_cutoff_ms"]
    target_state_last_ingest_seq = document["target_state_last_ingest_seq"]
    target_venue_ms = document["target_venue_ms"]
    assert isinstance(attempt_id, str)
    assert isinstance(signal_event_id, str)
    assert isinstance(symbol, str)
    assert isinstance(promoting_plan_sha256, str)
    assert isinstance(source_root_sha256, str)
    assert isinstance(lineage_sha256, str)
    assert isinstance(evidence_sha256, str)
    assert isinstance(invalidation, str)
    assert type(bar_open_ms) is int
    assert type(bar_close_ms) is int
    assert type(decision_cutoff_ms) is int
    assert type(target_state_last_ingest_seq) is int
    assert type(target_venue_ms) is int
    if target_venue_ms != target_cursor.target_venue_ms:
        raise PaperFokContractErrorV2("registry identity target differs from its cursor witness")
    try:
        decision = PaperFokEntryDecisionV2(
            attempt_id=attempt_id,
            signal_event_id=signal_event_id,
            symbol=symbol,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=promoting_plan_sha256,
            source_root_sha256=source_root_sha256,
            lineage_sha256=lineage_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
            target_cursor=target_cursor,
            target_state_last_ingest_seq=target_state_last_ingest_seq,
            side=side,
            requested_quantity=_decimal_from_document(document.get("requested_quantity")),
            status=status,
            inconclusive_cause=cause,
            closure_method=closure_method,
            certified_quantity=_optional_decimal_from_document(document.get("certified_quantity")),
            filled_quantity=_optional_decimal_from_document(document.get("filled_quantity")),
            executable_vwap=_optional_decimal_from_document(document.get("executable_vwap")),
            executable_notional=_optional_decimal_from_document(
                document.get("executable_notional")
            ),
            opposite_bbo=_optional_decimal_from_document(document.get("opposite_bbo")),
            paper_price_cap=_optional_decimal_from_document(document.get("paper_price_cap")),
            market_take_bound_price=_optional_decimal_from_document(
                document.get("market_take_bound_price")
            ),
            evidence_sha256=evidence_sha256,
            reasons=tuple(reasons),
            invalidation=invalidation,
            _factory_token=_DECISION_FACTORY_TOKEN,
        )
    except (PaperFokContractErrorV2, TypeError) as exc:
        raise PaperFokContractErrorV2(
            "registry decision fails constructor-equivalent validation"
        ) from exc
    if canonical_paper_fok_entry_decision_v2(decision) != canonical_json_line(document):
        raise PaperFokContractErrorV2("registry decision generated fields or hashes differ")
    return decision


def _target_cursor_from_document(value: object) -> CausalTargetCursorV2:
    expected_keys = {
        "clock_segment_root_sha256",
        "contiguous_cursor_evidence",
        "cursor_evidence_sha256",
        "decision_cutoff_ms",
        "prior_local_cursor_ms",
        "prior_venue_lower_bound_ms",
        "target_local_cursor_ms",
        "target_venue_lower_bound_ms",
        "target_venue_ms",
    }
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise PaperFokContractErrorV2("registry target cursor schema is not exact")
    clock_root = value.get("clock_segment_root_sha256")
    observed_evidence = value.get("cursor_evidence_sha256")
    if not isinstance(clock_root, str) or not isinstance(observed_evidence, str):
        raise PaperFokContractErrorV2("registry target cursor root is invalid")
    integer_names = (
        "decision_cutoff_ms",
        "prior_local_cursor_ms",
        "prior_venue_lower_bound_ms",
        "target_local_cursor_ms",
        "target_venue_lower_bound_ms",
        "target_venue_ms",
    )
    if any(type(value.get(name)) is not int for name in integer_names):
        raise PaperFokContractErrorV2("registry target cursor integer is invalid")
    contiguous_cursor_evidence = value.get("contiguous_cursor_evidence")
    if type(contiguous_cursor_evidence) is not bool:
        raise PaperFokContractErrorV2("registry target cursor flag is invalid")
    decision_cutoff_ms = value["decision_cutoff_ms"]
    prior_local_cursor_ms = value["prior_local_cursor_ms"]
    prior_venue_lower_bound_ms = value["prior_venue_lower_bound_ms"]
    target_local_cursor_ms = value["target_local_cursor_ms"]
    target_venue_lower_bound_ms = value["target_venue_lower_bound_ms"]
    target_venue_ms = value["target_venue_ms"]
    assert type(decision_cutoff_ms) is int
    assert type(prior_local_cursor_ms) is int
    assert type(prior_venue_lower_bound_ms) is int
    assert type(target_local_cursor_ms) is int
    assert type(target_venue_lower_bound_ms) is int
    assert type(target_venue_ms) is int
    try:
        cursor = CausalTargetCursorV2(
            decision_cutoff_ms=decision_cutoff_ms,
            target_venue_ms=target_venue_ms,
            prior_local_cursor_ms=prior_local_cursor_ms,
            prior_venue_lower_bound_ms=prior_venue_lower_bound_ms,
            target_local_cursor_ms=target_local_cursor_ms,
            target_venue_lower_bound_ms=target_venue_lower_bound_ms,
            clock_segment_root_sha256=clock_root,
            contiguous_cursor_evidence=contiguous_cursor_evidence,
        )
    except (TypeError, ValueError) as exc:
        raise PaperFokContractErrorV2("registry target cursor is invalid") from exc
    if observed_evidence != cursor.cursor_evidence_sha256:
        raise PaperFokContractErrorV2("registry target cursor evidence hash mismatch")
    return cursor


def _decimal_from_document(value: object) -> Decimal:
    if not isinstance(value, str):
        raise PaperFokContractErrorV2("canonical Decimal field must be text")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise PaperFokContractErrorV2("canonical Decimal text is invalid") from exc
    _validate_decimal(parsed, "canonical Decimal", allow_zero=False)
    if str(parsed) != value:
        raise PaperFokContractErrorV2("canonical Decimal text is noncanonical")
    return parsed


def _optional_decimal_from_document(value: object) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise PaperFokContractErrorV2("optional Decimal field must be text or null")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise PaperFokContractErrorV2("optional Decimal text is invalid") from exc
    _validate_decimal(parsed, "optional Decimal", allow_zero=True)
    if str(parsed) != value:
        raise PaperFokContractErrorV2("optional Decimal text is noncanonical")
    return parsed


def _decision_identity_document(
    decision: PaperFokEntryDecisionV2,
) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": decision.role,
        "rule_version": decision.rule_version,
        "side": decision.side.value,
        "signal_event_id": decision.signal_event_id,
        "symbol": decision.symbol,
        "target_venue_ms": decision.target_venue_ms,
        "venue": decision.venue.value,
    }


def _decision_document(
    decision: PaperFokEntryDecisionV2,
    *,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_decision_identity_document(decision),
        "bar_close_ms": decision.bar_close_ms,
        "certified_quantity": _optional_decimal_text(decision.certified_quantity),
        "closure_method": decision.closure_method.value,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "discord_timing_can_change_result": decision.discord_timing_can_change_result,
        "discord_timing_present": decision.discord_timing_present,
        "event_id": decision.event_id,
        "evidence_sha256": decision.evidence_sha256,
        "executable_notional": _optional_decimal_text(decision.executable_notional),
        "executable_vwap": _optional_decimal_text(decision.executable_vwap),
        "filled_quantity": _optional_decimal_text(decision.filled_quantity),
        "inconclusive_cause": (
            None if decision.inconclusive_cause is None else decision.inconclusive_cause.value
        ),
        "invalidation": decision.invalidation,
        "lineage_sha256": decision.lineage_sha256,
        "market_take_bound_price": _optional_decimal_text(decision.market_take_bound_price),
        "opposite_bbo": _optional_decimal_text(decision.opposite_bbo),
        "paper_price_cap": _optional_decimal_text(decision.paper_price_cap),
        "partial_primary_entry": decision.partial_primary_entry,
        "price_cap_rate": str(decision.price_cap_rate),
        "primary_depth_haircut": str(decision.primary_depth_haircut),
        "production_order_placement": decision.production_order_placement,
        "reasons": list(decision.reasons),
        "requested_quantity": str(decision.requested_quantity),
        "source_root_sha256": decision.source_root_sha256,
        "status": decision.status.value,
        "target_cursor": _target_cursor_document(decision.target_cursor),
        "target_state_last_ingest_seq": decision.target_state_last_ingest_seq,
    }
    if include_payload_sha256:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _evidence_document(
    item: PaperFokEntryInputV2,
    *,
    evaluation: _EvaluationV2,
) -> dict[str, object]:
    closure = item.closure
    successor_material, _ = _first_successor_material(closure)
    normalized_depth, _ = _normalize_pre_target_events(item.pre_target_depth_events)
    if successor_material:
        closure_finalized = item.target_local_cursor_ms
    elif evaluation.closure_method is PaperFokClosureMethodV2.PENDING:
        closure_finalized = closure.finalized_through_local_ms
    else:
        closure_finalized = closure.closure_grace_end_local_ms
    return {
        "attempt_id": item.attempt_id,
        "bar_close_ms": item.bar_close_ms,
        "bar_open_ms": item.bar_open_ms,
        "closure": {
            "closure_grace_end_local_ms": closure.closure_grace_end_local_ms,
            "finalization_grace_binding_sha256": (closure.finalization_grace_binding_sha256),
            "continuous_health": (
                None
                if successor_material or closure.continuous_health is None
                else _health_document(closure.continuous_health)
            ),
            "finalized_through_local_ms": closure_finalized,
            "quiet_rest_snapshot": (
                None
                if successor_material or closure.quiet_rest_snapshot is None
                else _quiet_snapshot_document(closure.quiet_rest_snapshot)
            ),
            "successor_candidates": [_witness_document(value) for value in successor_material],
        },
        "decision_cutoff_ms": item.decision_cutoff_ms,
        "exchange_info": _exchange_info_document(item.exchange_info),
        "lineage": _lineage_document(item.lineage),
        "mark": _mark_document(item.mark),
        "pre_target_depth_events": [_depth_event_document(value) for value in normalized_depth],
        "requested_quantity": str(item.requested_quantity),
        "schema_version": "r4b_paper_fok_entry_evidence_v2",
        "side": item.side.value,
        "signal_event_id": item.signal_event_id,
        "snapshot": _snapshot_document(item.snapshot),
        "symbol": item.symbol,
        "target_cursor": _target_cursor_document(item.target_cursor),
        "target_state_last_ingest_seq": item.target_state_last_ingest_seq,
        "venue": item.venue.value,
    }


def _target_cursor_document(value: CausalTargetCursorV2) -> dict[str, object]:
    return {
        "clock_segment_root_sha256": value.clock_segment_root_sha256,
        "contiguous_cursor_evidence": value.contiguous_cursor_evidence,
        "cursor_evidence_sha256": value.cursor_evidence_sha256,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "prior_local_cursor_ms": value.prior_local_cursor_ms,
        "prior_venue_lower_bound_ms": value.prior_venue_lower_bound_ms,
        "target_local_cursor_ms": value.target_local_cursor_ms,
        "target_venue_lower_bound_ms": value.target_venue_lower_bound_ms,
        "target_venue_ms": value.target_venue_ms,
    }


def _certificate_document(
    value: PaperFokFullFillCertificateV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "decision_event_id": value.decision_event_id,
        "decision_payload_sha256": value.decision_payload_sha256,
        "evidence_sha256": value.evidence_sha256,
        "executable_notional": str(value.executable_notional),
        "executable_vwap": str(value.executable_vwap),
        "filled_quantity": str(value.filled_quantity),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "requested_quantity": str(value.requested_quantity),
        "role": "PAPER_FOK_FULL_FILL_CERTIFICATE",
        "rule_version": PAPER_FOK_RULE_VERSION_V2,
        "signal_event_id": value.signal_event_id,
        "side": value.side.value,
        "symbol": value.symbol,
        "target_cursor": _target_cursor_document(value.target_cursor),
        "terminal_registry_checkpoint_sha256": (value.terminal_registry_checkpoint_sha256),
        "terminal_registry_event_count": value.terminal_registry_event_count,
        "terminal_registry_maximum_events": (value.terminal_registry_maximum_events),
        "terminal_registry_replay_root_sha256": (value.terminal_registry_replay_root_sha256),
        "venue": value.venue.value,
    }


def _lineage_document(value: PaperFokLineageV2) -> dict[str, object]:
    return {
        "depth_snapshot_schema_sha256": value.depth_snapshot_schema_sha256,
        "exchange_info_schema_sha256": value.exchange_info_schema_sha256,
        "health_schema_sha256": value.health_schema_sha256,
        "mark_schema_sha256": value.mark_schema_sha256,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "source_root_sha256": value.source_root_sha256,
        "standard_depth_schema_sha256": value.standard_depth_schema_sha256,
    }


def _row_base_document(value: _EvidenceRowV2) -> dict[str, object]:
    venue = value.venue
    if not isinstance(venue, VenueV2):  # pragma: no cover - row constructors enforce
        raise PaperFokContractErrorV2("row venue has the wrong type")
    return {
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": venue.value,
    }


def _snapshot_document(value: FuturesDepthSnapshotV2) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "asks": _levels_document(value.asks),
        "bids": _levels_document(value.bids),
        "depth_limit": value.depth_limit,
        "last_update_id": value.last_update_id,
        "response_completion_ms": value.response_completion_ms,
    }


def _depth_event_document(value: FuturesStandardDepthEventV2) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "asks": _levels_document(value.asks),
        "bids": _levels_document(value.bids),
        "event_time_ms": value.event_time_ms,
        "final_update_id": value.final_update_id,
        "first_update_id": value.first_update_id,
        "ingest_seq": value.ingest_seq,
        "pair": value.pair,
        "previous_final_update_id": value.previous_final_update_id,
        "previous_same_stream_ingest_seq": value.previous_same_stream_ingest_seq,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
        "transaction_time_ms": value.transaction_time_ms,
    }


def _witness_document(value: FuturesDepthContinuityWitnessV2) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "event_time_ms": value.event_time_ms,
        "final_update_id": value.final_update_id,
        "first_update_id": value.first_update_id,
        "ingest_seq": value.ingest_seq,
        "pair": value.pair,
        "previous_final_update_id": value.previous_final_update_id,
        "previous_same_stream_ingest_seq": value.previous_same_stream_ingest_seq,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
        "transaction_time_ms": value.transaction_time_ms,
    }


def _quiet_snapshot_document(
    value: QuietRestSnapshotEvidenceV2,
) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "last_update_id": value.last_update_id,
        "response_completion_ms": value.response_completion_ms,
    }


def _health_document(value: ContinuousBookHealthEvidenceV2) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "disconnect_count": value.disconnect_count,
        "generation": value.generation,
        "interval_end_local_ms": value.interval_end_local_ms,
        "interval_start_local_ms": value.interval_start_local_ms,
        "parser_error_count": value.parser_error_count,
        "queue_drop_count": value.queue_drop_count,
        "sequence_gap_count": value.sequence_gap_count,
    }


def _mark_document(value: CausalMarkPriceEvidenceV2) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "event_time_ms": value.event_time_ms,
        "mark_price": str(value.mark_price),
        "pair": value.pair,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
    }


def _exchange_info_document(
    value: FuturesExchangeInfoEvidenceV2,
) -> dict[str, object]:
    return {
        **_row_base_document(value),
        "applicable_filter_inventory_complete": (value.applicable_filter_inventory_complete),
        "lot_size": _quantity_filter_document(value.lot_size),
        "market_lot_size": _quantity_filter_document(value.market_lot_size),
        "market_take_bound": str(value.market_take_bound),
        "max_notional": str(value.max_notional),
        "max_price": str(value.max_price),
        "min_notional": str(value.min_notional),
        "min_price": str(value.min_price),
        "percent_price_multiplier_down": _optional_decimal_text(
            value.percent_price_multiplier_down
        ),
        "percent_price_multiplier_up": _optional_decimal_text(value.percent_price_multiplier_up),
        "response_completion_ms": value.response_completion_ms,
        "tick_size": str(value.tick_size),
        "version_valid_from_local_ms": value.version_valid_from_local_ms,
        "version_valid_through_local_ms": value.version_valid_through_local_ms,
    }


def _quantity_filter_document(
    value: RawQuantityFilterV2 | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "max_qty": str(value.max_qty),
        "min_qty": str(value.min_qty),
        "step_size": str(value.step_size),
    }


def _levels_document(values: tuple[DepthLevelV2, ...]) -> list[dict[str, str]]:
    return [
        {"price": str(value.price), "quantity": str(value.quantity)}
        for value in sorted(values, key=lambda level: level.price)
    ]


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _validate_decision_quantities(decision: PaperFokEntryDecisionV2) -> None:
    execution_values = (
        decision.certified_quantity,
        decision.filled_quantity,
        decision.executable_vwap,
        decision.executable_notional,
        decision.opposite_bbo,
        decision.paper_price_cap,
        decision.market_take_bound_price,
    )
    if decision.status in (
        PaperFokEntryStatusV2.CLOSURE_PENDING,
        PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
        PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
    ):
        if any(value is not None for value in execution_values):
            raise PaperFokContractErrorV2(
                "pending/inconclusive decision cannot expose execution values"
            )
        if (
            decision.status is PaperFokEntryStatusV2.CLOSURE_PENDING
            and decision.closure_method is not PaperFokClosureMethodV2.PENDING
        ):
            raise PaperFokContractErrorV2("CLOSURE_PENDING requires PENDING method")
        return
    if decision.closure_method not in (
        PaperFokClosureMethodV2.CONTIGUOUS_SUCCESSOR,
        PaperFokClosureMethodV2.QUIET_REST_EQUAL,
    ):
        raise PaperFokContractErrorV2("terminal capacity decision requires proven causal closure")
    assert decision.certified_quantity is not None
    assert decision.filled_quantity is not None
    assert decision.opposite_bbo is not None
    assert decision.paper_price_cap is not None
    assert decision.market_take_bound_price is not None
    _validate_decimal(
        decision.certified_quantity,
        "certified_quantity",
        allow_zero=True,
    )
    _validate_decimal(decision.filled_quantity, "filled_quantity", allow_zero=True)
    _validate_decimal(decision.opposite_bbo, "opposite_bbo", allow_zero=False)
    _validate_decimal(decision.paper_price_cap, "paper_price_cap", allow_zero=False)
    _validate_decimal(
        decision.market_take_bound_price,
        "market_take_bound_price",
        allow_zero=True,
    )
    if decision.status is PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL:
        if (
            decision.certified_quantity != 0
            or decision.filled_quantity != 0
            or decision.executable_vwap is not None
            or decision.executable_notional is not None
        ):
            raise PaperFokContractErrorV2("no-fill quantity state is contradictory")
        return
    if decision.status is PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY:
        if not (
            0 < decision.certified_quantity < decision.requested_quantity
            and decision.filled_quantity == 0
            and decision.executable_vwap is None
            and decision.executable_notional is None
        ):
            raise PaperFokContractErrorV2("capacity-reject quantity state is invalid")
        return
    if decision.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY:
        raise PaperFokContractErrorV2("unsupported terminal PAPER status")
    if decision.executable_vwap is None or decision.executable_notional is None:
        raise PaperFokContractErrorV2("full PAPER fill requires executable VWAP and notional")
    _validate_decimal(decision.executable_vwap, "executable_vwap", allow_zero=False)
    _validate_decimal(
        decision.executable_notional,
        "executable_notional",
        allow_zero=False,
    )
    if not (
        decision.certified_quantity >= decision.requested_quantity
        and decision.filled_quantity == decision.requested_quantity
    ):
        raise PaperFokContractErrorV2("full PAPER fill is not full quantity")


def _validate_levels(
    values: tuple[DepthLevelV2, ...],
    field_name: str,
    *,
    allow_zero_quantity: bool,
) -> None:
    if type(values) is not tuple or any(not isinstance(value, DepthLevelV2) for value in values):
        raise PaperFokContractErrorV2(f"{field_name} must be an immutable DepthLevelV2 tuple")
    if len({value.price for value in values}) != len(values):
        raise PaperFokContractErrorV2(f"{field_name} repeats a price")
    if not allow_zero_quantity and any(value.quantity == 0 for value in values):
        raise PaperFokContractErrorV2(f"{field_name} snapshot quantities must be positive")


def _validate_row_identity(value: object) -> None:
    _validate_symbol_shape(getattr(value, "symbol", None), "symbol")
    if not isinstance(getattr(value, "venue", None), VenueV2):
        raise PaperFokContractErrorV2("row venue must be VenueV2")
    for field_name in (
        "promoting_plan_sha256",
        "source_root_sha256",
        "schema_sha256",
    ):
        raw = getattr(value, field_name, None)
        if not isinstance(raw, str):
            raise PaperFokContractErrorV2(f"{field_name} must be text")
        _validate_sha256(raw, field_name)


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise PaperFokContractErrorV2("reasons must be a non-empty bounded immutable tuple")
    for value in values:
        _validate_identity(value, "reason")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise PaperFokContractErrorV2(f"{field_name} must be a nonnegative integer")


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise PaperFokContractErrorV2(f"{field_name} must be a bounded normalized identity")


def _validate_symbol_shape(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise PaperFokContractErrorV2(f"{field_name} must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PaperFokContractErrorV2(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_sha256_value(value: object, field_name: str) -> None:
    if not isinstance(value, str):
        raise PaperFokContractErrorV2(f"{field_name} must be text")
    _validate_sha256(value, field_name)
