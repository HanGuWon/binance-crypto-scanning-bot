from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from fractions import Fraction
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    FIVE_MINUTE_MS_V2,
    DecisionClockContractErrorV2,
    validate_decision_bar_v2,
)
from signalbot.r4b_v2.protocol.features import (
    ROBUST_Z_PRIOR_WINDOW_V2,
    RobustZStatusV2,
    robust_z_v2,
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_FEATURE_HASH_DOMAIN: Final = b"R4B_FAMILY_B_FEATURE_EVIDENCE_V2\0"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_FAMILY_B_SOURCE_ROOT_V2\0"
_SLICE_ROOT_DOMAIN: Final = b"R4B_FAMILY_B_SLICE_ROOT_V2\0"
_ROW_HASH_DOMAIN: Final = b"R4B_FAMILY_B_RAW_ROW_V2\0"
_EXIT_FEATURE_HASH_DOMAIN: Final = b"R4B_FAMILY_B_EXIT_FEATURE_EVIDENCE_V2\0"
_EXIT_SOURCE_ROOT_DOMAIN: Final = b"R4B_FAMILY_B_EXIT_SOURCE_ROOT_V2\0"
_FLOW_ONLY_BAR_HASH_DOMAIN: Final = b"R4B_FAMILY_B_FLOW_ONLY_BAR_EVIDENCE_V2\0"
_FLOW_ONLY_SOURCE_ROOT_DOMAIN: Final = b"R4B_FAMILY_B_FLOW_ONLY_SOURCE_ROOT_V2\0"
_TEN_BPS: Final = Decimal("0.001")
_FACTORY_TOKEN: Final = object()


class FamilyBFeatureContractErrorV2(ValueError):
    """Raised when Family B evidence is not causal, complete, or authoritative."""


class FamilyBFeatureReadinessV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_WARMUP = "FEATURE_NOT_READY_WARMUP"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    FEATURE_NOT_READY_DEPTH = "FEATURE_NOT_READY_DEPTH"


class FamilyBFlowOnlyBarReadinessV2(StrEnum):
    """Readiness of one exact closed normal-flow projection."""

    READY = "READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"


class FamilyBBookSideV2(StrEnum):
    BID = "BID"
    ASK = "ASK"


class FamilyBBookSourceV2(StrEnum):
    STANDARD_DIFF_DEPTH = "STANDARD_DIFF_DEPTH"
    RPI = "RPI"


@dataclass(frozen=True, slots=True)
class FamilyBWeightedObservationV2:
    """One finite state value and its positive causal duration in milliseconds."""

    value: Decimal
    duration_ms: int

    def __post_init__(self) -> None:
        if not _is_finite_decimal(self.value):
            raise FamilyBFeatureContractErrorV2(
                "weighted observation value must be finite Decimal"
            )
        _validate_positive_int(self.duration_ms, "duration_ms")


@dataclass(frozen=True, slots=True)
class FamilyBFeatureSourceLineageV2:
    """Frozen capture/schema roots from which one Family B bar is derived."""

    book_capture_root_sha256: str
    normal_flow_capture_root_sha256: str
    kline_capture_root_sha256: str
    prior_feature_capture_root_sha256: str
    book_schema_sha256: str
    normal_flow_nq_schema_sha256: str
    kline_schema_sha256: str
    prior_feature_schema_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "book_capture_root_sha256",
            "normal_flow_capture_root_sha256",
            "kline_capture_root_sha256",
            "prior_feature_capture_root_sha256",
            "book_schema_sha256",
            "normal_flow_nq_schema_sha256",
            "kline_schema_sha256",
            "prior_feature_schema_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)

    @property
    def root_sha256(self) -> str:
        return hashlib.sha256(
            _SOURCE_ROOT_DOMAIN + canonical_json_line(_lineage_document(self))
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class FamilyBBookLevelV2:
    """One active raw price/quantity level from a standard diff-depth book."""

    side: FamilyBBookSideV2
    price: Decimal
    quantity: Decimal
    contract_multiplier: Decimal = Decimal(1)

    def __post_init__(self) -> None:
        if not isinstance(self.side, FamilyBBookSideV2):
            raise FamilyBFeatureContractErrorV2("book level side must be BID or ASK")
        if not _is_positive_finite(self.price) or not _is_positive_finite(
            self.quantity
        ):
            raise FamilyBFeatureContractErrorV2(
                "active raw book level price and quantity must be positive finite Decimal"
            )
        if not _is_positive_finite(self.contract_multiplier):
            raise FamilyBFeatureContractErrorV2(
                "book contract_multiplier must be positive finite Decimal"
            )


@dataclass(frozen=True, slots=True)
class FamilyBBookStateV2:
    """Factory-sealed sequence row containing raw standard diff-depth levels."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    capture_root_sha256: str
    schema_sha256: str
    source: FamilyBBookSourceV2
    transaction_time_ms: int
    receipt_ms: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    levels: tuple[FamilyBBookLevelV2, ...]
    source_evidence_sha256: str
    _factory_token: InitVar[object] = None
    row_payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "book state must be created by its raw-row factory"
            )
        _validate_row_identity(
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            capture_root_sha256=self.capture_root_sha256,
            schema_sha256=self.schema_sha256,
        )
        if not isinstance(self.source, FamilyBBookSourceV2):
            raise FamilyBFeatureContractErrorV2("book source is unsupported")
        _validate_nonnegative_int(self.transaction_time_ms, "transaction_time_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if self.transaction_time_ms > self.receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "book transaction time cannot follow its local receipt"
            )
        _validate_nonnegative_int(self.first_update_id, "first_update_id")
        _validate_nonnegative_int(self.final_update_id, "final_update_id")
        if self.first_update_id > self.final_update_id:
            raise FamilyBFeatureContractErrorV2(
                "first_update_id cannot exceed final_update_id"
            )
        if self.previous_final_update_id is not None:
            _validate_nonnegative_int(
                self.previous_final_update_id,
                "previous_final_update_id",
            )
        if type(self.levels) is not tuple or any(
            not isinstance(value, FamilyBBookLevelV2) for value in self.levels
        ):
            raise FamilyBFeatureContractErrorV2(
                "levels must be an immutable tuple of raw book levels"
            )
        if self.levels != _order_book_levels(self.levels):
            raise FamilyBFeatureContractErrorV2(
                "raw book levels must use canonical side/price order"
            )
        if len({(value.side, value.price) for value in self.levels}) != len(
            self.levels
        ):
            raise FamilyBFeatureContractErrorV2(
                "duplicate raw book side/price levels are forbidden"
            )
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")
        _validate_raw_book_geometry(self)
        object.__setattr__(
            self,
            "row_payload_sha256",
            _row_hash("BOOK_STATE", _book_state_document(self, include_row_hash=False)),
        )

    @classmethod
    def create(
        cls,
        *,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        capture_root_sha256: str,
        schema_sha256: str,
        source: FamilyBBookSourceV2,
        transaction_time_ms: int,
        receipt_ms: int,
        first_update_id: int,
        final_update_id: int,
        previous_final_update_id: int | None,
        levels: tuple[FamilyBBookLevelV2, ...],
        source_evidence_sha256: str,
    ) -> FamilyBBookStateV2:
        return cls(
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            source=source,
            transaction_time_ms=transaction_time_ms,
            receipt_ms=receipt_ms,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            previous_final_update_id=previous_final_update_id,
            levels=_order_book_levels(levels),
            source_evidence_sha256=source_evidence_sha256,
            _factory_token=_FACTORY_TOKEN,
        )

    @property
    def best_bid(self) -> Decimal:
        return max(
            value.price for value in self.levels if value.side is FamilyBBookSideV2.BID
        )

    @property
    def best_ask(self) -> Decimal:
        return min(
            value.price for value in self.levels if value.side is FamilyBBookSideV2.ASK
        )

    @property
    def bid_depth_10bp_quote(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            lower = self.best_bid * (Decimal(1) - _TEN_BPS)
        return _raw_depth_quote(self.levels, FamilyBBookSideV2.BID, lower)

    @property
    def ask_depth_10bp_quote(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            upper = self.best_ask * (Decimal(1) + _TEN_BPS)
        return _raw_depth_quote(self.levels, FamilyBBookSideV2.ASK, upper)

    @property
    def complete_10bp_band(self) -> bool:
        bids = tuple(
            value.price for value in self.levels if value.side is FamilyBBookSideV2.BID
        )
        asks = tuple(
            value.price for value in self.levels if value.side is FamilyBBookSideV2.ASK
        )
        with localcontext(protocol_decimal_context_v2()):
            lower = self.best_bid * (Decimal(1) - _TEN_BPS)
            upper = self.best_ask * (Decimal(1) + _TEN_BPS)
        return min(bids) <= lower and max(asks) >= upper

    @property
    def mid(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return (self.best_bid + self.best_ask) / Decimal(2)

    @property
    def spread_bps(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return Decimal(10_000) * (self.best_ask - self.best_bid) / self.mid


@dataclass(frozen=True, slots=True)
class FamilyBNormalFlowTradeV2:
    """One normal-quantity USD-M trade retained with exact nq schema lineage."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    capture_root_sha256: str
    schema_sha256: str
    trade_id: int
    transaction_time_ms: int
    receipt_ms: int
    price: Decimal
    quantity: Decimal
    normal_quantity: Decimal
    contract_multiplier: Decimal
    buyer_maker: bool
    source_evidence_sha256: str
    _factory_token: InitVar[object] = None
    row_payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "normal-flow trade must be created by its raw-row factory"
            )
        _validate_row_identity(
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            capture_root_sha256=self.capture_root_sha256,
            schema_sha256=self.schema_sha256,
        )
        _validate_nonnegative_int(self.trade_id, "trade_id")
        _validate_nonnegative_int(self.transaction_time_ms, "transaction_time_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if self.transaction_time_ms > self.receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "normal-flow transaction time cannot follow its local receipt"
            )
        if not _is_positive_finite(self.price):
            raise FamilyBFeatureContractErrorV2(
                "normal-flow price must be positive finite Decimal"
            )
        if not _is_nonnegative_finite(self.quantity) or not _is_nonnegative_finite(
            self.normal_quantity
        ):
            raise FamilyBFeatureContractErrorV2(
                "q and nq must be nonnegative finite Decimal"
            )
        if self.normal_quantity > self.quantity:
            raise FamilyBFeatureContractErrorV2("normal-flow nq cannot exceed q")
        if not _is_positive_finite(self.contract_multiplier):
            raise FamilyBFeatureContractErrorV2(
                "contract_multiplier must be positive finite Decimal"
            )
        if type(self.buyer_maker) is not bool:
            raise FamilyBFeatureContractErrorV2("buyer_maker must be boolean")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")
        object.__setattr__(
            self,
            "row_payload_sha256",
            _row_hash("NORMAL_FLOW_TRADE", _normal_flow_document(self, include_row_hash=False)),
        )

    @classmethod
    def create(
        cls,
        *,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        capture_root_sha256: str,
        schema_sha256: str,
        trade_id: int,
        transaction_time_ms: int,
        receipt_ms: int,
        price: Decimal,
        quantity: Decimal,
        normal_quantity: Decimal,
        contract_multiplier: Decimal,
        buyer_maker: bool,
        source_evidence_sha256: str,
    ) -> FamilyBNormalFlowTradeV2:
        return cls(
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            trade_id=trade_id,
            transaction_time_ms=transaction_time_ms,
            receipt_ms=receipt_ms,
            price=price,
            quantity=quantity,
            normal_quantity=normal_quantity,
            contract_multiplier=contract_multiplier,
            buyer_maker=buyer_maker,
            source_evidence_sha256=source_evidence_sha256,
            _factory_token=_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class FamilyBPriorBarFeaturesV2:
    """One sealed prior bar used by both exact 8,640-value robust-z windows."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    capture_root_sha256: str
    schema_sha256: str
    bar_open_ms: int
    flow_imbalance: Decimal
    bar_return: Decimal
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    source_evidence_sha256: str
    _factory_token: InitVar[object] = None
    row_payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "prior feature must be created by its sealed-row factory"
            )
        _validate_row_identity(
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            capture_root_sha256=self.capture_root_sha256,
            schema_sha256=self.schema_sha256,
        )
        _validate_nonnegative_int(self.bar_open_ms, "bar_open_ms")
        if self.bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyBFeatureContractErrorV2(
                "prior feature bar must align to a 5m UTC boundary"
            )
        if not _is_finite_decimal(self.flow_imbalance) or abs(
            self.flow_imbalance
        ) > 1:
            raise FamilyBFeatureContractErrorV2(
                "prior flow imbalance must be finite in [-1, 1]"
            )
        if not _is_finite_decimal(self.bar_return):
            raise FamilyBFeatureContractErrorV2(
                "prior bar return must be finite Decimal"
            )
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        own_bar_close_ms = self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        if self.latest_source_event_ms < own_bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "prior feature source event cannot precede its own bar close"
            )
        if self.latest_source_event_ms > self.latest_source_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "prior feature source event cannot follow its receipt"
            )
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")
        object.__setattr__(
            self,
            "row_payload_sha256",
            _row_hash("PRIOR_FEATURE", _prior_feature_document(self, include_row_hash=False)),
        )

    @classmethod
    def create(
        cls,
        *,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        capture_root_sha256: str,
        schema_sha256: str,
        bar_open_ms: int,
        flow_imbalance: Decimal,
        bar_return: Decimal,
        latest_source_event_ms: int,
        latest_source_receipt_ms: int,
        source_evidence_sha256: str,
    ) -> FamilyBPriorBarFeaturesV2:
        return cls(
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            bar_open_ms=bar_open_ms,
            flow_imbalance=flow_imbalance,
            bar_return=bar_return,
            latest_source_event_ms=latest_source_event_ms,
            latest_source_receipt_ms=latest_source_receipt_ms,
            source_evidence_sha256=source_evidence_sha256,
            _factory_token=_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class FamilyBFlowWindowClosureV2:
    """Sealed proof that the normal-flow source completed an exact bar."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    capture_root_sha256: str
    schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    complete_through_event_ms: int
    closure_event_ms: int
    closure_receipt_ms: int
    source_evidence_sha256: str
    _factory_token: InitVar[object] = None
    row_payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "flow-window closure must be created by its sealed-row factory"
            )
        _validate_row_identity(
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            capture_root_sha256=self.capture_root_sha256,
            schema_sha256=self.schema_sha256,
        )
        _validate_nonnegative_int(self.bar_open_ms, "bar_open_ms")
        _validate_nonnegative_int(self.bar_close_ms, "bar_close_ms")
        _validate_nonnegative_int(
            self.complete_through_event_ms, "complete_through_event_ms"
        )
        _validate_nonnegative_int(self.closure_event_ms, "closure_event_ms")
        _validate_nonnegative_int(self.closure_receipt_ms, "closure_receipt_ms")
        if (
            self.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
            or self.bar_close_ms != self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        ):
            raise FamilyBFeatureContractErrorV2(
                "flow-window closure must bind an exact aligned 5m bar"
            )
        if self.complete_through_event_ms != self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "flow-window closure must bind data completeness through exact k.T"
            )
        if self.closure_event_ms <= self.complete_through_event_ms:
            raise FamilyBFeatureContractErrorV2(
                "flow completeness is first observable at k.T+1 or later"
            )
        if self.closure_event_ms > self.closure_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "flow closure event cannot follow its local receipt"
            )
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")
        object.__setattr__(
            self,
            "row_payload_sha256",
            _row_hash("FLOW_WINDOW_CLOSURE", _flow_closure_document(self, include_row_hash=False)),
        )

    @classmethod
    def create(
        cls,
        *,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        capture_root_sha256: str,
        schema_sha256: str,
        bar_open_ms: int,
        bar_close_ms: int,
        complete_through_event_ms: int,
        closure_event_ms: int,
        closure_receipt_ms: int,
        source_evidence_sha256: str,
    ) -> FamilyBFlowWindowClosureV2:
        return cls(
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            complete_through_event_ms=complete_through_event_ms,
            closure_event_ms=closure_event_ms,
            closure_receipt_ms=closure_receipt_ms,
            source_evidence_sha256=source_evidence_sha256,
            _factory_token=_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class FamilyBFlowOnlyBarEvidenceV2:
    """Factory-sealed projection of one complete closed normal-flow bar.

    This owner deliberately excludes price-return and order-book features.  Its
    source slice still binds every canonical trade row and the exact flow-window
    closure from which the notional projection was derived.  Readiness is exact
    relative to those supplied Family B rows; it does not claim M0 signed raw
    membership, M1 source parsing, or M2 causal cursor finality.
    """

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    normal_flow_capture_root_sha256: str
    normal_flow_nq_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    normal_flow_slice_sha256: str
    flow_source_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    readiness: FamilyBFlowOnlyBarReadinessV2
    signed_normal_notional: Decimal
    normal_notional: Decimal
    total_trade_notional: Decimal
    flow_imbalance: Decimal | None
    signed_share: Decimal | None
    reasons: tuple[str, ...]
    _factory_token: InitVar[object] = None
    flow_bar_evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "flow-only bar evidence must be created by its causal factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBFeatureContractErrorV2(
                "flow-only bar evidence requires USD-M Futures provenance"
            )
        for value, field_name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (
                self.normal_flow_capture_root_sha256,
                "normal_flow_capture_root_sha256",
            ),
            (
                self.normal_flow_nq_schema_sha256,
                "normal_flow_nq_schema_sha256",
            ),
            (self.normal_flow_slice_sha256, "normal_flow_slice_sha256"),
            (self.flow_source_root_sha256, "flow_source_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_bar(self.bar_open_ms, self.bar_close_ms, self.decision_cutoff_ms)
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        if self.latest_source_event_ms <= self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "flow-only evidence requires a post-k.T completeness observation"
            )
        if self.latest_source_event_ms > self.latest_source_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "flow-only source event cannot follow its receipt"
            )
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "source receipt after D cannot enter flow-only bar evidence"
            )
        if not isinstance(self.readiness, FamilyBFlowOnlyBarReadinessV2):
            raise FamilyBFeatureContractErrorV2(
                "flow-only readiness must use FamilyBFlowOnlyBarReadinessV2"
            )
        for value, field_name in (
            (self.signed_normal_notional, "signed_normal_notional"),
            (self.normal_notional, "normal_notional"),
            (self.total_trade_notional, "total_trade_notional"),
        ):
            if not _is_finite_decimal(value):
                raise FamilyBFeatureContractErrorV2(
                    f"{field_name} must be finite Decimal"
                )
        if self.normal_notional < 0 or self.total_trade_notional < 0:
            raise FamilyBFeatureContractErrorV2(
                "normal and total trade notional must be nonnegative"
            )
        if abs(self.signed_normal_notional) > self.normal_notional:
            raise FamilyBFeatureContractErrorV2(
                "absolute signed normal notional cannot exceed normal notional"
            )
        if self.normal_notional > self.total_trade_notional:
            raise FamilyBFeatureContractErrorV2(
                "normal notional cannot exceed total trade notional"
            )
        expected_readiness = (
            FamilyBFlowOnlyBarReadinessV2.INCONCLUSIVE_DATA
            if self.normal_notional == 0 or self.total_trade_notional == 0
            else FamilyBFlowOnlyBarReadinessV2.READY
        )
        if self.readiness is not expected_readiness:
            raise FamilyBFeatureContractErrorV2(
                "flow-only readiness contradicts the sealed notionals"
            )
        if self.readiness is FamilyBFlowOnlyBarReadinessV2.READY:
            if not _is_finite_decimal(self.flow_imbalance) or not _is_finite_decimal(
                self.signed_share
            ):
                raise FamilyBFeatureContractErrorV2(
                    "READY flow-only evidence requires finite directional ratios"
                )
            with localcontext(protocol_decimal_context_v2()):
                expected_imbalance = (
                    self.signed_normal_notional / self.normal_notional
                )
                expected_share = (
                    self.signed_normal_notional / self.total_trade_notional
                )
            if (
                self.flow_imbalance != expected_imbalance
                or self.signed_share != expected_share
            ):
                raise FamilyBFeatureContractErrorV2(
                    "flow-only directional ratios contradict sealed notionals"
                )
        elif self.flow_imbalance is not None or self.signed_share is not None:
            raise FamilyBFeatureContractErrorV2(
                "inconclusive flow-only evidence cannot expose directional ratios"
            )
        if (
            type(self.reasons) is not tuple
            or not self.reasons
            or len(self.reasons) > 8
            or any(
                not isinstance(value, str)
                or not value
                or value.strip() != value
                or len(value) > 128
                for value in self.reasons
            )
        ):
            raise FamilyBFeatureContractErrorV2(
                "flow-only reasons must be a non-empty bounded tuple"
            )
        expected_source_root = _flow_only_source_root(
            promoting_plan_sha256=self.promoting_plan_sha256,
            normal_flow_capture_root_sha256=(
                self.normal_flow_capture_root_sha256
            ),
            normal_flow_nq_schema_sha256=self.normal_flow_nq_schema_sha256,
            normal_flow_slice_sha256=self.normal_flow_slice_sha256,
        )
        if self.flow_source_root_sha256 != expected_source_root:
            raise FamilyBFeatureContractErrorV2(
                "flow_source_root_sha256 differs from its exact flow slice lineage"
            )
        object.__setattr__(
            self,
            "flow_bar_evidence_sha256",
            hashlib.sha256(
                _FLOW_ONLY_BAR_HASH_DOMAIN
                + canonical_json_line(_flow_only_bar_document(self))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class FamilyBKlineBarV2:
    """Factory-sealed exact 5m Binance kline row, including closure proof."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    capture_root_sha256: str
    schema_sha256: str
    interval_ms: int
    bar_open_ms: int
    bar_close_ms: int
    open_event_id: str
    close_event_id: str
    closed: bool
    event_ms: int
    receipt_ms: int
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Decimal
    source_evidence_sha256: str
    _factory_token: InitVar[object] = None
    row_payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "kline row must be created by its raw-row factory"
            )
        _validate_row_identity(
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            capture_root_sha256=self.capture_root_sha256,
            schema_sha256=self.schema_sha256,
        )
        _validate_positive_int(self.interval_ms, "interval_ms")
        _validate_nonnegative_int(self.bar_open_ms, "bar_open_ms")
        _validate_nonnegative_int(self.bar_close_ms, "bar_close_ms")
        _validate_identity(self.open_event_id, "open_event_id")
        _validate_identity(self.close_event_id, "close_event_id")
        if type(self.closed) is not bool:
            raise FamilyBFeatureContractErrorV2("closed must be boolean")
        _validate_nonnegative_int(self.event_ms, "event_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if not all(
            _is_positive_finite(value)
            for value in (self.high, self.low, self.close, self.previous_close)
        ) or not self.low <= self.close <= self.high:
            raise FamilyBFeatureContractErrorV2(
                "kline prices must be positive with low <= close <= high"
            )
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")
        object.__setattr__(
            self,
            "row_payload_sha256",
            _row_hash("KLINE_BAR", _kline_document(self, include_row_hash=False)),
        )

    @classmethod
    def create(
        cls,
        *,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        capture_root_sha256: str,
        schema_sha256: str,
        interval_ms: int,
        bar_open_ms: int,
        bar_close_ms: int,
        open_event_id: str,
        close_event_id: str,
        closed: bool,
        event_ms: int,
        receipt_ms: int,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        previous_close: Decimal,
        source_evidence_sha256: str,
    ) -> FamilyBKlineBarV2:
        return cls(
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            interval_ms=interval_ms,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            open_event_id=open_event_id,
            close_event_id=close_event_id,
            closed=closed,
            event_ms=event_ms,
            receipt_ms=receipt_ms,
            high=high,
            low=low,
            close=close,
            previous_close=previous_close,
            source_evidence_sha256=source_evidence_sha256,
            _factory_token=_FACTORY_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class FamilyBExitSourceLineageV2:
    normal_flow_capture_root_sha256: str
    kline_capture_root_sha256: str
    normal_flow_nq_schema_sha256: str
    kline_schema_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "normal_flow_capture_root_sha256",
            "kline_capture_root_sha256",
            "normal_flow_nq_schema_sha256",
            "kline_schema_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)

    @property
    def root_sha256(self) -> str:
        return hashlib.sha256(
            _EXIT_SOURCE_ROOT_DOMAIN
            + canonical_json_line(_exit_lineage_document(self))
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class FamilyBFeatureEvidenceV2:
    """Factory-sealed causal feature evidence accepted by the Family B evaluator."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    source_lineage: FamilyBFeatureSourceLineageV2
    book_slice_sha256: str
    normal_flow_slice_sha256: str
    prior_feature_slice_sha256: str
    kline_slice_sha256: str
    feature_source_root_sha256: str
    kline_event_ms: int
    kline_receipt_ms: int
    kline_evidence_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    readiness: FamilyBFeatureReadinessV2
    flow_rz_status: RobustZStatusV2
    bar_return_rz_status: RobustZStatusV2
    flow_imbalance_current: Decimal
    bar_return_current: Decimal
    rz_flow_imbalance_current: Decimal | None
    rz_bar_return_current: Decimal | None
    d_start: Decimal
    d_low: Decimal
    d_end: Decimal
    spread95_bps: Decimal
    high_current: Decimal
    low_current: Decimal
    previous_close: Decimal
    _factory_token: InitVar[object] = None
    feature_evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "Family B feature evidence must be created by its causal factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBFeatureContractErrorV2(
                "Family B feature evidence requires USD-M Futures provenance"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar(self.bar_open_ms, self.bar_close_ms, self.decision_cutoff_ms)
        if not isinstance(self.source_lineage, FamilyBFeatureSourceLineageV2):
            raise FamilyBFeatureContractErrorV2(
                "source_lineage must be FamilyBFeatureSourceLineageV2"
            )
        for field_name in (
            "book_slice_sha256",
            "normal_flow_slice_sha256",
            "prior_feature_slice_sha256",
            "kline_slice_sha256",
            "feature_source_root_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        _validate_nonnegative_int(self.kline_event_ms, "kline_event_ms")
        _validate_nonnegative_int(self.kline_receipt_ms, "kline_receipt_ms")
        _validate_sha256(self.kline_evidence_sha256, "kline_evidence_sha256")
        if self.kline_event_ms < self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "closed kline publication event cannot precede k.T"
            )
        if self.kline_event_ms > self.kline_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "kline publication event cannot follow its receipt"
            )
        if self.kline_receipt_ms > self.decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "kline receipt after D cannot enter Family B evidence"
            )
        expected_source_root = _feature_source_root(
            source_lineage=self.source_lineage,
            book_slice_sha256=self.book_slice_sha256,
            normal_flow_slice_sha256=self.normal_flow_slice_sha256,
            prior_feature_slice_sha256=self.prior_feature_slice_sha256,
            kline_slice_sha256=self.kline_slice_sha256,
        )
        if self.feature_source_root_sha256 != expected_source_root:
            raise FamilyBFeatureContractErrorV2(
                "feature_source_root_sha256 differs from bound slice lineage"
            )
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        if self.latest_source_event_ms <= self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "Family B evidence requires a post-k.T completeness observation"
            )
        if self.latest_source_event_ms > self.latest_source_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "latest source event cannot follow its receipt"
            )
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "source receipt after D cannot enter Family B evidence"
            )
        if not isinstance(self.readiness, FamilyBFeatureReadinessV2):
            raise FamilyBFeatureContractErrorV2(
                "readiness must be FamilyBFeatureReadinessV2"
            )
        if not isinstance(self.flow_rz_status, RobustZStatusV2) or not isinstance(
            self.bar_return_rz_status,
            RobustZStatusV2,
        ):
            raise FamilyBFeatureContractErrorV2(
                "robust-z statuses must use RobustZStatusV2"
            )
        for value, field_name in (
            (self.flow_imbalance_current, "flow_imbalance_current"),
            (self.bar_return_current, "bar_return_current"),
            (self.d_start, "d_start"),
            (self.d_low, "d_low"),
            (self.d_end, "d_end"),
            (self.spread95_bps, "spread95_bps"),
            (self.high_current, "high_current"),
            (self.low_current, "low_current"),
            (self.previous_close, "previous_close"),
        ):
            if not _is_finite_decimal(value):
                raise FamilyBFeatureContractErrorV2(
                    f"{field_name} must be finite Decimal"
                )
        if abs(self.flow_imbalance_current) > 1:
            raise FamilyBFeatureContractErrorV2(
                "flow_imbalance_current must be in [-1, 1]"
            )
        if any(
            value < 0
            for value in (
                self.d_start,
                self.d_low,
                self.d_end,
                self.spread95_bps,
            )
        ):
            raise FamilyBFeatureContractErrorV2(
                "depth and spread features must be nonnegative"
            )
        if not all(
            _is_positive_finite(value)
            for value in (self.high_current, self.low_current, self.previous_close)
        ) or self.low_current > self.high_current:
            raise FamilyBFeatureContractErrorV2(
                "kline true-range inputs must be positive and ordered"
            )
        ready_rz = (
            self.flow_rz_status is RobustZStatusV2.READY
            and self.bar_return_rz_status is RobustZStatusV2.READY
        )
        if ready_rz:
            if not _is_finite_decimal(
                self.rz_flow_imbalance_current
            ) or not _is_finite_decimal(self.rz_bar_return_current):
                raise FamilyBFeatureContractErrorV2(
                    "READY robust-z evidence requires both finite values"
                )
        elif self.rz_flow_imbalance_current is not None or self.rz_bar_return_current is not None:
            raise FamilyBFeatureContractErrorV2(
                "non-ready robust-z evidence cannot expose partial values"
            )
        expected_readiness = _combined_readiness(
            flow_status=self.flow_rz_status,
            bar_return_status=self.bar_return_rz_status,
            d_start=self.d_start,
            d_low=self.d_low,
        )
        if self.readiness is not expected_readiness:
            raise FamilyBFeatureContractErrorV2(
                "feature readiness contradicts robust-z/depth evidence"
            )
        digest = hashlib.sha256(
            _FEATURE_HASH_DOMAIN + canonical_json_line(_feature_document(self))
        ).hexdigest()
        object.__setattr__(self, "feature_evidence_sha256", digest)


@dataclass(frozen=True, slots=True)
class FamilyBExitFeatureEvidenceV2:
    """Factory-sealed causal evidence accepted by the Family B exit evaluator."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    source_lineage: FamilyBExitSourceLineageV2
    normal_flow_slice_sha256: str
    kline_slice_sha256: str
    exit_source_root_sha256: str
    kline_event_ms: int
    kline_receipt_ms: int
    kline_evidence_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    flow_imbalance_current: Decimal
    close_price: Decimal
    _factory_token: InitVar[object] = None
    exit_evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyBFeatureContractErrorV2(
                "Family B exit evidence must be created by its causal factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBFeatureContractErrorV2(
                "Family B exit evidence requires USD-M Futures provenance"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar(self.bar_open_ms, self.bar_close_ms, self.decision_cutoff_ms)
        if not isinstance(self.source_lineage, FamilyBExitSourceLineageV2):
            raise FamilyBFeatureContractErrorV2(
                "source_lineage must be FamilyBExitSourceLineageV2"
            )
        for field_name in (
            "normal_flow_slice_sha256",
            "kline_slice_sha256",
            "exit_source_root_sha256",
            "kline_evidence_sha256",
        ):
            _validate_sha256(getattr(self, field_name), field_name)
        expected_root = _exit_feature_source_root(
            source_lineage=self.source_lineage,
            normal_flow_slice_sha256=self.normal_flow_slice_sha256,
            kline_slice_sha256=self.kline_slice_sha256,
        )
        if self.exit_source_root_sha256 != expected_root:
            raise FamilyBFeatureContractErrorV2(
                "exit_source_root_sha256 differs from bound slice lineage"
            )
        for value, field_name in (
            (self.kline_event_ms, "kline_event_ms"),
            (self.kline_receipt_ms, "kline_receipt_ms"),
            (self.latest_source_event_ms, "latest_source_event_ms"),
            (self.latest_source_receipt_ms, "latest_source_receipt_ms"),
        ):
            _validate_nonnegative_int(value, field_name)
        if self.kline_event_ms < self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "closed exit kline publication event cannot precede k.T"
            )
        if self.kline_event_ms > self.kline_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "exit kline publication event cannot follow its receipt"
            )
        if self.latest_source_event_ms <= self.bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "Family B exit evidence requires a post-k.T completeness observation"
            )
        if self.latest_source_event_ms > self.latest_source_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "latest exit source event cannot follow its receipt"
            )
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "source receipt after exit D cannot enter Family B exit evidence"
            )
        if not _is_finite_decimal(self.flow_imbalance_current) or abs(
            self.flow_imbalance_current
        ) > 1:
            raise FamilyBFeatureContractErrorV2(
                "exit flow imbalance must be finite in [-1, 1]"
            )
        if not _is_positive_finite(self.close_price):
            raise FamilyBFeatureContractErrorV2(
                "exit close price must be positive finite Decimal"
            )
        object.__setattr__(
            self,
            "exit_evidence_sha256",
            hashlib.sha256(
                _EXIT_FEATURE_HASH_DOMAIN
                + canonical_json_line(_exit_feature_document(self))
            ).hexdigest(),
        )


def duration_weighted_quantile_v2(
    observations: tuple[FamilyBWeightedObservationV2, ...],
    quantile: Decimal,
) -> Decimal:
    """Return the smallest x whose exact duration CDF reaches q."""

    if type(observations) is not tuple or not observations:
        raise FamilyBFeatureContractErrorV2(
            "weighted quantile requires a non-empty tuple"
        )
    if any(not isinstance(item, FamilyBWeightedObservationV2) for item in observations):
        raise FamilyBFeatureContractErrorV2(
            "weighted quantile requires FamilyBWeightedObservationV2 values"
        )
    if not _is_finite_decimal(quantile) or quantile <= 0 or quantile > 1:
        raise FamilyBFeatureContractErrorV2(
            "quantile must be finite Decimal in (0, 1]"
        )
    q = Fraction(quantile)
    total_duration = sum(item.duration_ms for item in observations)
    cumulative_duration = 0
    for item in sorted(observations, key=lambda value: value.value):
        cumulative_duration += item.duration_ms
        if cumulative_duration * q.denominator >= total_duration * q.numerator:
            return item.value
    raise FamilyBFeatureContractErrorV2(
        "weighted quantile CDF did not reach its target"
    )


def build_family_b_flow_only_bar_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    normal_flow_capture_root_sha256: str,
    normal_flow_nq_schema_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    normal_flow_trades: tuple[FamilyBNormalFlowTradeV2, ...],
    flow_window_closure: FamilyBFlowWindowClosureV2,
) -> FamilyBFlowOnlyBarEvidenceV2:
    """Project one complete closed aggTrade window without price/book features."""

    _validate_identity(attempt_id, "attempt_id")
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise FamilyBFeatureContractErrorV2(
            "flow-only bar evidence accepts USD-M Futures only"
        )
    for value, field_name in (
        (promoting_plan_sha256, "promoting_plan_sha256"),
        (normal_flow_capture_root_sha256, "normal_flow_capture_root_sha256"),
        (normal_flow_nq_schema_sha256, "normal_flow_nq_schema_sha256"),
    ):
        _validate_sha256(value, field_name)
    _validate_bar(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    _validate_flow_window_closure(
        flow_window_closure,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=normal_flow_capture_root_sha256,
        schema_sha256=normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    ordered_trades = _validate_and_order_normal_flow(
        normal_flow_trades,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=normal_flow_capture_root_sha256,
        schema_sha256=normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    signed_normal, normal_notional, total_notional = _normal_flow_notionals(
        ordered_trades
    )
    readiness = (
        FamilyBFlowOnlyBarReadinessV2.INCONCLUSIVE_DATA
        if normal_notional == 0 or total_notional == 0
        else FamilyBFlowOnlyBarReadinessV2.READY
    )
    flow_imbalance: Decimal | None = None
    signed_share: Decimal | None = None
    reasons: tuple[str, ...]
    if readiness is FamilyBFlowOnlyBarReadinessV2.READY:
        flow_imbalance = _normal_flow_imbalance(ordered_trades)
        with localcontext(protocol_decimal_context_v2()):
            signed_share = signed_normal / total_notional
        reasons = ("EXACT_CLOSED_NORMAL_FLOW_BAR_READY",)
    else:
        reasons = ("NORMAL_FLOW_NOTIONAL_DENOMINATOR_ZERO",)

    normal_flow_slice_sha256 = _slice_hash(
        "NORMAL_FLOW_TRADES",
        [
            *(
                _normal_flow_document(value, include_row_hash=True)
                for value in ordered_trades
            ),
            _flow_closure_document(flow_window_closure, include_row_hash=True),
        ],
    )
    source_root = _flow_only_source_root(
        promoting_plan_sha256=promoting_plan_sha256,
        normal_flow_capture_root_sha256=normal_flow_capture_root_sha256,
        normal_flow_nq_schema_sha256=normal_flow_nq_schema_sha256,
        normal_flow_slice_sha256=normal_flow_slice_sha256,
    )
    return FamilyBFlowOnlyBarEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        normal_flow_capture_root_sha256=normal_flow_capture_root_sha256,
        normal_flow_nq_schema_sha256=normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        normal_flow_slice_sha256=normal_flow_slice_sha256,
        flow_source_root_sha256=source_root,
        latest_source_event_ms=max(
            [
                flow_window_closure.closure_event_ms,
                *(value.transaction_time_ms for value in ordered_trades),
            ]
        ),
        latest_source_receipt_ms=max(
            [
                flow_window_closure.closure_receipt_ms,
                *(value.receipt_ms for value in ordered_trades),
            ]
        ),
        readiness=readiness,
        signed_normal_notional=signed_normal,
        normal_notional=normal_notional,
        total_trade_notional=total_notional,
        flow_imbalance=flow_imbalance,
        signed_share=signed_share,
        reasons=reasons,
        _factory_token=_FACTORY_TOKEN,
    )


def build_family_b_feature_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    source_lineage: FamilyBFeatureSourceLineageV2,
    book_states: tuple[FamilyBBookStateV2, ...],
    normal_flow_trades: tuple[FamilyBNormalFlowTradeV2, ...],
    prior_features: tuple[FamilyBPriorBarFeaturesV2, ...],
    flow_window_closure: FamilyBFlowWindowClosureV2,
    kline_bar: FamilyBKlineBarV2,
) -> FamilyBFeatureEvidenceV2:
    """Build the only scalar evidence accepted by the Family B entry evaluator."""

    _validate_identity(attempt_id, "attempt_id")
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise FamilyBFeatureContractErrorV2(
            "Family B promoting features accept USD-M Futures only"
        )
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    _validate_bar(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    if not isinstance(source_lineage, FamilyBFeatureSourceLineageV2):
        raise FamilyBFeatureContractErrorV2(
            "source_lineage must be FamilyBFeatureSourceLineageV2"
        )
    _validate_flow_window_closure(
        flow_window_closure,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.normal_flow_capture_root_sha256,
        schema_sha256=source_lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    _validate_kline_bar(
        kline_bar,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.kline_capture_root_sha256,
        schema_sha256=source_lineage.kline_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )

    ordered_states = _validate_and_order_book_states(
        book_states,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.book_capture_root_sha256,
        schema_sha256=source_lineage.book_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    ordered_trades = _validate_and_order_normal_flow(
        normal_flow_trades,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.normal_flow_capture_root_sha256,
        schema_sha256=source_lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    ordered_prior = _validate_and_order_prior_features(
        prior_features,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.prior_feature_capture_root_sha256,
        schema_sha256=source_lineage.prior_feature_schema_sha256,
        current_bar_open_ms=bar_open_ms,
        current_decision_cutoff_ms=decision_cutoff_ms,
    )
    flow_imbalance = _normal_flow_imbalance(ordered_trades)
    windows = _duration_windows(
        ordered_states,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        flow_sign=_sign(flow_imbalance),
    )
    first_mid = duration_weighted_quantile_v2(windows.first_5s_mid, Decimal("0.5"))
    last_mid = duration_weighted_quantile_v2(windows.last_5s_mid, Decimal("0.5"))
    with localcontext(protocol_decimal_context_v2()):
        bar_return = (last_mid / first_mid).ln()
    d_start = duration_weighted_quantile_v2(
        windows.first_30s_opposing_depth,
        Decimal("0.5"),
    )
    d_low = duration_weighted_quantile_v2(
        windows.full_bar_opposing_depth,
        Decimal("0.05"),
    )
    d_end = duration_weighted_quantile_v2(
        windows.last_30s_opposing_depth,
        Decimal("0.5"),
    )
    spread95 = duration_weighted_quantile_v2(
        windows.full_bar_spread_bps,
        Decimal("0.95"),
    )

    flow_rz = robust_z_v2(
        tuple(value.flow_imbalance for value in ordered_prior),
        flow_imbalance,
    )
    return_rz = robust_z_v2(
        tuple(value.bar_return for value in ordered_prior),
        bar_return,
    )
    readiness = _combined_readiness(
        flow_status=flow_rz.status,
        bar_return_status=return_rz.status,
        d_start=d_start,
        d_low=d_low,
    )
    rz_flow: Decimal | None = None
    rz_return: Decimal | None = None
    if (
        flow_rz.status is RobustZStatusV2.READY
        and return_rz.status is RobustZStatusV2.READY
    ):
        assert flow_rz.value is not None
        assert return_rz.value is not None
        rz_flow = flow_rz.value
        rz_return = return_rz.value

    book_slice_sha256 = _slice_hash(
        "BOOK_STATES",
        [
            _book_state_document(value, include_row_hash=True)
            for value in ordered_states
        ],
    )
    flow_slice_sha256 = _slice_hash(
        "NORMAL_FLOW_TRADES",
        [
            *(
                _normal_flow_document(value, include_row_hash=True)
                for value in ordered_trades
            ),
            _flow_closure_document(flow_window_closure, include_row_hash=True),
        ],
    )
    prior_slice_sha256 = _slice_hash(
        "PRIOR_FEATURES",
        [
            _prior_feature_document(value, include_row_hash=True)
            for value in ordered_prior
        ],
    )
    kline_slice_sha256 = _slice_hash(
        "KLINE_TRUE_RANGE",
        [_kline_document(kline_bar, include_row_hash=True)],
    )
    feature_source_root_sha256 = _feature_source_root(
        source_lineage=source_lineage,
        book_slice_sha256=book_slice_sha256,
        normal_flow_slice_sha256=flow_slice_sha256,
        prior_feature_slice_sha256=prior_slice_sha256,
        kline_slice_sha256=kline_slice_sha256,
    )
    source_event_times = [
        kline_bar.event_ms,
        flow_window_closure.closure_event_ms,
        *(value.transaction_time_ms for value in ordered_states),
        *(value.transaction_time_ms for value in ordered_trades),
        *(value.latest_source_event_ms for value in ordered_prior),
    ]
    source_receipt_times = [
        kline_bar.receipt_ms,
        flow_window_closure.closure_receipt_ms,
        *(value.receipt_ms for value in ordered_states),
        *(value.receipt_ms for value in ordered_trades),
        *(value.latest_source_receipt_ms for value in ordered_prior),
    ]
    return FamilyBFeatureEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        source_lineage=source_lineage,
        book_slice_sha256=book_slice_sha256,
        normal_flow_slice_sha256=flow_slice_sha256,
        prior_feature_slice_sha256=prior_slice_sha256,
        kline_slice_sha256=kline_slice_sha256,
        feature_source_root_sha256=feature_source_root_sha256,
        kline_event_ms=kline_bar.event_ms,
        kline_receipt_ms=kline_bar.receipt_ms,
        kline_evidence_sha256=kline_bar.row_payload_sha256,
        latest_source_event_ms=max(source_event_times),
        latest_source_receipt_ms=max(source_receipt_times),
        readiness=readiness,
        flow_rz_status=flow_rz.status,
        bar_return_rz_status=return_rz.status,
        flow_imbalance_current=flow_imbalance,
        bar_return_current=bar_return,
        rz_flow_imbalance_current=rz_flow,
        rz_bar_return_current=rz_return,
        d_start=d_start,
        d_low=d_low,
        d_end=d_end,
        spread95_bps=spread95,
        high_current=kline_bar.high,
        low_current=kline_bar.low,
        previous_close=kline_bar.previous_close,
        _factory_token=_FACTORY_TOKEN,
    )


def build_family_b_exit_feature_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    source_lineage: FamilyBExitSourceLineageV2,
    normal_flow_trades: tuple[FamilyBNormalFlowTradeV2, ...],
    flow_window_closure: FamilyBFlowWindowClosureV2,
    kline_bar: FamilyBKlineBarV2,
) -> FamilyBExitFeatureEvidenceV2:
    """Build a closed-bar exit feature object from row-bound causal inputs."""

    _validate_identity(attempt_id, "attempt_id")
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise FamilyBFeatureContractErrorV2(
            "Family B exit features accept USD-M Futures only"
        )
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    _validate_bar(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    if not isinstance(source_lineage, FamilyBExitSourceLineageV2):
        raise FamilyBFeatureContractErrorV2(
            "source_lineage must be FamilyBExitSourceLineageV2"
        )
    _validate_flow_window_closure(
        flow_window_closure,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.normal_flow_capture_root_sha256,
        schema_sha256=source_lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    _validate_kline_bar(
        kline_bar,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.kline_capture_root_sha256,
        schema_sha256=source_lineage.kline_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    ordered_trades = _validate_and_order_normal_flow(
        normal_flow_trades,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=source_lineage.normal_flow_capture_root_sha256,
        schema_sha256=source_lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    normal_flow_slice_sha256 = _slice_hash(
        "EXIT_NORMAL_FLOW_TRADES",
        [
            *(
                _normal_flow_document(value, include_row_hash=True)
                for value in ordered_trades
            ),
            _flow_closure_document(flow_window_closure, include_row_hash=True),
        ],
    )
    kline_slice_sha256 = _slice_hash(
        "EXIT_KLINE_CLOSE",
        [_kline_document(kline_bar, include_row_hash=True)],
    )
    exit_source_root_sha256 = _exit_feature_source_root(
        source_lineage=source_lineage,
        normal_flow_slice_sha256=normal_flow_slice_sha256,
        kline_slice_sha256=kline_slice_sha256,
    )
    event_times = [
        kline_bar.event_ms,
        flow_window_closure.closure_event_ms,
        *(value.transaction_time_ms for value in ordered_trades),
    ]
    receipt_times = [
        kline_bar.receipt_ms,
        flow_window_closure.closure_receipt_ms,
        *(value.receipt_ms for value in ordered_trades),
    ]
    return FamilyBExitFeatureEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        source_lineage=source_lineage,
        normal_flow_slice_sha256=normal_flow_slice_sha256,
        kline_slice_sha256=kline_slice_sha256,
        exit_source_root_sha256=exit_source_root_sha256,
        kline_event_ms=kline_bar.event_ms,
        kline_receipt_ms=kline_bar.receipt_ms,
        kline_evidence_sha256=kline_bar.row_payload_sha256,
        latest_source_event_ms=max(event_times),
        latest_source_receipt_ms=max(receipt_times),
        flow_imbalance_current=_normal_flow_imbalance(ordered_trades),
        close_price=kline_bar.close,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_family_b_flow_only_bar_evidence_v2(
    evidence: FamilyBFlowOnlyBarEvidenceV2,
) -> bytes:
    if not isinstance(evidence, FamilyBFlowOnlyBarEvidenceV2):
        raise FamilyBFeatureContractErrorV2(
            "evidence must be FamilyBFlowOnlyBarEvidenceV2"
        )
    payload = canonical_json_line(_flow_only_bar_document(evidence))
    expected = hashlib.sha256(_FLOW_ONLY_BAR_HASH_DOMAIN + payload).hexdigest()
    if evidence.flow_bar_evidence_sha256 != expected:
        raise FamilyBFeatureContractErrorV2(
            "flow-only bar evidence hash differs from canonical content"
        )
    return payload


def canonical_family_b_feature_evidence_v2(
    evidence: FamilyBFeatureEvidenceV2,
) -> bytes:
    if not isinstance(evidence, FamilyBFeatureEvidenceV2):
        raise FamilyBFeatureContractErrorV2(
            "evidence must be FamilyBFeatureEvidenceV2"
        )
    expected = hashlib.sha256(
        _FEATURE_HASH_DOMAIN + canonical_json_line(_feature_document(evidence))
    ).hexdigest()
    if evidence.feature_evidence_sha256 != expected:
        raise FamilyBFeatureContractErrorV2(
            "feature evidence hash differs from canonical payload"
        )
    document = _feature_document(evidence)
    document["feature_evidence_sha256"] = evidence.feature_evidence_sha256
    return canonical_json_line(document)


def canonical_family_b_kline_bar_v2(bar: FamilyBKlineBarV2) -> bytes:
    """Return the canonical, self-hash-checked raw kline row."""

    if not isinstance(bar, FamilyBKlineBarV2):
        raise FamilyBFeatureContractErrorV2("bar must be FamilyBKlineBarV2")
    expected = _row_hash(
        "KLINE_BAR",
        _kline_document(bar, include_row_hash=False),
    )
    if bar.row_payload_sha256 != expected:
        raise FamilyBFeatureContractErrorV2(
            "kline row payload hash differs from canonical content"
        )
    return canonical_json_line(_kline_document(bar, include_row_hash=True))


def canonical_family_b_exit_feature_evidence_v2(
    evidence: FamilyBExitFeatureEvidenceV2,
) -> bytes:
    if not isinstance(evidence, FamilyBExitFeatureEvidenceV2):
        raise FamilyBFeatureContractErrorV2(
            "evidence must be FamilyBExitFeatureEvidenceV2"
        )
    expected = hashlib.sha256(
        _EXIT_FEATURE_HASH_DOMAIN
        + canonical_json_line(_exit_feature_document(evidence))
    ).hexdigest()
    if evidence.exit_evidence_sha256 != expected:
        raise FamilyBFeatureContractErrorV2(
            "exit evidence hash differs from canonical payload"
        )
    document = _exit_feature_document(evidence)
    document["exit_evidence_sha256"] = evidence.exit_evidence_sha256
    return canonical_json_line(document)


@dataclass(frozen=True, slots=True)
class _DurationWindowsV2:
    first_5s_mid: tuple[FamilyBWeightedObservationV2, ...]
    last_5s_mid: tuple[FamilyBWeightedObservationV2, ...]
    first_30s_opposing_depth: tuple[FamilyBWeightedObservationV2, ...]
    full_bar_opposing_depth: tuple[FamilyBWeightedObservationV2, ...]
    last_30s_opposing_depth: tuple[FamilyBWeightedObservationV2, ...]
    full_bar_spread_bps: tuple[FamilyBWeightedObservationV2, ...]


def _duration_windows(
    states: tuple[FamilyBBookStateV2, ...],
    *,
    bar_open_ms: int,
    bar_close_ms: int,
    flow_sign: int,
) -> _DurationWindowsV2:
    bar_end_ms = bar_close_ms + 1
    first_5_end = bar_open_ms + 5_000
    last_5_start = bar_end_ms - 5_000
    first_30_end = bar_open_ms + 30_000
    last_30_start = bar_end_ms - 30_000
    first_5_mid: list[FamilyBWeightedObservationV2] = []
    last_5_mid: list[FamilyBWeightedObservationV2] = []
    first_30_depth: list[FamilyBWeightedObservationV2] = []
    full_depth: list[FamilyBWeightedObservationV2] = []
    last_30_depth: list[FamilyBWeightedObservationV2] = []
    full_spread: list[FamilyBWeightedObservationV2] = []

    for index, state in enumerate(states):
        state_end = (
            states[index + 1].transaction_time_ms
            if index + 1 < len(states)
            else bar_end_ms
        )
        clipped_start = max(state.transaction_time_ms, bar_open_ms)
        clipped_end = min(state_end, bar_end_ms)
        if clipped_end <= clipped_start:
            continue
        opposing_depth = (
            state.ask_depth_10bp_quote
            if flow_sign > 0
            else state.bid_depth_10bp_quote
            if flow_sign < 0
            else Decimal(0)
        )
        _append_clipped(
            first_5_mid,
            state.mid,
            clipped_start,
            clipped_end,
            bar_open_ms,
            first_5_end,
        )
        _append_clipped(
            last_5_mid,
            state.mid,
            clipped_start,
            clipped_end,
            last_5_start,
            bar_end_ms,
        )
        _append_clipped(
            first_30_depth,
            opposing_depth,
            clipped_start,
            clipped_end,
            bar_open_ms,
            first_30_end,
        )
        _append_clipped(
            full_depth,
            opposing_depth,
            clipped_start,
            clipped_end,
            bar_open_ms,
            bar_end_ms,
        )
        _append_clipped(
            last_30_depth,
            opposing_depth,
            clipped_start,
            clipped_end,
            last_30_start,
            bar_end_ms,
        )
        _append_clipped(
            full_spread,
            state.spread_bps,
            clipped_start,
            clipped_end,
            bar_open_ms,
            bar_end_ms,
        )

    result = _DurationWindowsV2(
        first_5s_mid=tuple(first_5_mid),
        last_5s_mid=tuple(last_5_mid),
        first_30s_opposing_depth=tuple(first_30_depth),
        full_bar_opposing_depth=tuple(full_depth),
        last_30s_opposing_depth=tuple(last_30_depth),
        full_bar_spread_bps=tuple(full_spread),
    )
    for observations, expected, name in (
        (result.first_5s_mid, 5_000, "first_5s"),
        (result.last_5s_mid, 5_000, "last_5s"),
        (result.first_30s_opposing_depth, 30_000, "first_30s"),
        (result.full_bar_opposing_depth, FIVE_MINUTE_MS_V2, "full_bar_depth"),
        (result.last_30s_opposing_depth, 30_000, "last_30s"),
        (result.full_bar_spread_bps, FIVE_MINUTE_MS_V2, "full_bar_spread"),
    ):
        if sum(value.duration_ms for value in observations) != expected:
            raise FamilyBFeatureContractErrorV2(
                f"{name} lacks exact positive-duration full coverage"
            )
    return result


def _append_clipped(
    output: list[FamilyBWeightedObservationV2],
    value: Decimal,
    state_start: int,
    state_end: int,
    window_start: int,
    window_end: int,
) -> None:
    duration = min(state_end, window_end) - max(state_start, window_start)
    if duration > 0:
        output.append(FamilyBWeightedObservationV2(value, duration))


def _validate_and_order_book_states(
    states: tuple[FamilyBBookStateV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> tuple[FamilyBBookStateV2, ...]:
    if type(states) is not tuple or not states or any(
        not isinstance(value, FamilyBBookStateV2) for value in states
    ):
        raise FamilyBFeatureContractErrorV2(
            "book_states must be a non-empty immutable tuple of book states"
        )
    ordered = tuple(
        sorted(states, key=lambda value: (value.transaction_time_ms, value.final_update_id))
    )
    carried = tuple(value for value in ordered if value.transaction_time_ms < bar_open_ms)
    at_open = tuple(value for value in ordered if value.transaction_time_ms == bar_open_ms)
    if len(carried) > 1 or (carried and ordered[0] is not carried[0]):
        raise FamilyBFeatureContractErrorV2(
            "book slice permits at most one carried pre-open state"
        )
    if not carried and not at_open:
        raise FamilyBFeatureContractErrorV2(
            "book slice requires a carried or exact-open state"
        )
    seen_final_ids: set[int] = set()
    for index, state in enumerate(ordered):
        _validate_row_membership(
            state,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            row_name="book state",
        )
        if state.source is not FamilyBBookSourceV2.STANDARD_DIFF_DEPTH:
            raise FamilyBFeatureContractErrorV2(
                "RPI or non-standard book rows are forbidden"
            )
        _verify_row_hash(
            state.row_payload_sha256,
            "BOOK_STATE",
            _book_state_document(state, include_row_hash=False),
        )
        if not state.complete_10bp_band:
            raise FamilyBFeatureContractErrorV2(
                "raw standard book levels do not prove a complete 10bp two-sided band"
            )
        if state.transaction_time_ms > bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "future book state after k.T is forbidden"
            )
        if state.transaction_time_ms > state.receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "book transaction time cannot follow its local receipt"
            )
        if state.receipt_ms > decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "book state received after D is forbidden"
            )
        if state.final_update_id in seen_final_ids:
            raise FamilyBFeatureContractErrorV2(
                "duplicate final_update_id is not a sequence-valid state"
            )
        seen_final_ids.add(state.final_update_id)
        if index == 0:
            continue
        previous = ordered[index - 1]
        if state.previous_final_update_id != previous.final_update_id:
            raise FamilyBFeatureContractErrorV2(
                "Futures book continuity requires next.pu == prior.u"
            )
        if state.final_update_id <= previous.final_update_id:
            raise FamilyBFeatureContractErrorV2(
                "Futures final update IDs must increase strictly"
            )
    return ordered


def _validate_and_order_normal_flow(
    trades: tuple[FamilyBNormalFlowTradeV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> tuple[FamilyBNormalFlowTradeV2, ...]:
    if type(trades) is not tuple or any(
        not isinstance(value, FamilyBNormalFlowTradeV2) for value in trades
    ):
        raise FamilyBFeatureContractErrorV2(
            "normal_flow_trades must be an immutable tuple"
        )
    ordered = tuple(
        sorted(trades, key=lambda value: (value.transaction_time_ms, value.trade_id))
    )
    trade_ids: set[int] = set()
    for trade in ordered:
        _validate_row_membership(
            trade,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            row_name="normal-flow trade",
        )
        _verify_row_hash(
            trade.row_payload_sha256,
            "NORMAL_FLOW_TRADE",
            _normal_flow_document(trade, include_row_hash=False),
        )
        if not bar_open_ms <= trade.transaction_time_ms <= bar_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "normal-flow trade must lie inside the exact closed bar"
            )
        if trade.transaction_time_ms > trade.receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "normal-flow transaction time cannot follow its local receipt"
            )
        if trade.receipt_ms > decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "normal-flow trade received after D is forbidden"
            )
        if trade.trade_id in trade_ids:
            raise FamilyBFeatureContractErrorV2(
                "duplicate normal-flow trade_id is forbidden"
            )
        trade_ids.add(trade.trade_id)
    return ordered


def _validate_and_order_prior_features(
    values: tuple[FamilyBPriorBarFeaturesV2, ...],
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    current_bar_open_ms: int,
    current_decision_cutoff_ms: int,
) -> tuple[FamilyBPriorBarFeaturesV2, ...]:
    if type(values) is not tuple or any(
        not isinstance(value, FamilyBPriorBarFeaturesV2) for value in values
    ):
        raise FamilyBFeatureContractErrorV2(
            "prior_features must be an immutable tuple"
        )
    if len(values) > ROBUST_Z_PRIOR_WINDOW_V2:
        raise FamilyBFeatureContractErrorV2(
            "prior feature window cannot exceed 8,640 bars"
        )
    ordered = tuple(sorted(values, key=lambda value: value.bar_open_ms))
    if len({value.bar_open_ms for value in ordered}) != len(ordered):
        raise FamilyBFeatureContractErrorV2(
            "prior feature bars must be unique"
        )
    expected_start = current_bar_open_ms - len(ordered) * FIVE_MINUTE_MS_V2
    for index, value in enumerate(ordered):
        _validate_row_membership(
            value,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            row_name="prior feature",
        )
        _verify_row_hash(
            value.row_payload_sha256,
            "PRIOR_FEATURE",
            _prior_feature_document(value, include_row_hash=False),
        )
        expected_open = expected_start + index * FIVE_MINUTE_MS_V2
        if value.bar_open_ms != expected_open:
            raise FamilyBFeatureContractErrorV2(
                "prior feature bars must be exact contiguous t-W through t-1"
            )
        prior_close_ms = value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        if value.latest_source_event_ms < prior_close_ms:
            raise FamilyBFeatureContractErrorV2(
                "prior feature source event cannot precede its own bar close"
            )
        if value.latest_source_event_ms > value.latest_source_receipt_ms:
            raise FamilyBFeatureContractErrorV2(
                "prior feature source event cannot follow its receipt"
            )
        if value.latest_source_receipt_ms > current_decision_cutoff_ms:
            raise FamilyBFeatureContractErrorV2(
                "prior feature receipt after current D is forbidden"
            )
    return ordered


def _normal_flow_imbalance(
    trades: tuple[FamilyBNormalFlowTradeV2, ...],
) -> Decimal:
    signed_normal, normal_notional, _ = _normal_flow_notionals(trades)
    if normal_notional == 0:
        return Decimal(0)
    with localcontext(protocol_decimal_context_v2()):
        return signed_normal / normal_notional


def _normal_flow_notionals(
    trades: tuple[FamilyBNormalFlowTradeV2, ...],
) -> tuple[Decimal, Decimal, Decimal]:
    """Return signed normal, absolute normal, and total trade notionals."""

    with localcontext(protocol_decimal_context_v2()):
        normal_buy = sum(
            (
                value.price * value.normal_quantity * value.contract_multiplier
                for value in trades
                if not value.buyer_maker
            ),
            Decimal(0),
        )
        normal_sell = sum(
            (
                value.price * value.normal_quantity * value.contract_multiplier
                for value in trades
                if value.buyer_maker
            ),
            Decimal(0),
        )
        total = sum(
            (
                value.price * value.quantity * value.contract_multiplier
                for value in trades
            ),
            Decimal(0),
        )
        return normal_buy - normal_sell, normal_buy + normal_sell, total


def _combined_readiness(
    *,
    flow_status: RobustZStatusV2,
    bar_return_status: RobustZStatusV2,
    d_start: Decimal,
    d_low: Decimal,
) -> FamilyBFeatureReadinessV2:
    if d_start == 0 or d_low == 0:
        return FamilyBFeatureReadinessV2.FEATURE_NOT_READY_DEPTH
    statuses = (flow_status, bar_return_status)
    if any(value is RobustZStatusV2.DATA_INVALID_FEATURE for value in statuses):
        raise FamilyBFeatureContractErrorV2(
            "nonfinite robust-z inputs cannot create Family B evidence"
        )
    if any(value is RobustZStatusV2.FEATURE_NOT_READY_WARMUP for value in statuses):
        return FamilyBFeatureReadinessV2.FEATURE_NOT_READY_WARMUP
    if any(value is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE for value in statuses):
        return FamilyBFeatureReadinessV2.FEATURE_NOT_READY_ZERO_SCALE
    return FamilyBFeatureReadinessV2.READY


def _validate_flow_window_closure(
    value: FamilyBFlowWindowClosureV2,
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    if not isinstance(value, FamilyBFlowWindowClosureV2):
        raise FamilyBFeatureContractErrorV2(
            "flow_window_closure must be FamilyBFlowWindowClosureV2"
        )
    _validate_row_membership(
        value,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=capture_root_sha256,
        schema_sha256=schema_sha256,
        row_name="flow-window closure",
    )
    _verify_row_hash(
        value.row_payload_sha256,
        "FLOW_WINDOW_CLOSURE",
        _flow_closure_document(value, include_row_hash=False),
    )
    if (value.bar_open_ms, value.bar_close_ms) != (bar_open_ms, bar_close_ms):
        raise FamilyBFeatureContractErrorV2(
            "flow-window closure does not bind the exact decision bar"
        )
    if value.complete_through_event_ms != bar_close_ms:
        raise FamilyBFeatureContractErrorV2(
            "flow-window closure must prove completeness through exact k.T"
        )
    if value.closure_event_ms <= value.complete_through_event_ms:
        raise FamilyBFeatureContractErrorV2(
            "flow completeness must be observed at k.T+1 or later"
        )
    if value.closure_event_ms > value.closure_receipt_ms:
        raise FamilyBFeatureContractErrorV2(
            "flow closure event cannot follow its local receipt"
        )
    if value.closure_receipt_ms > decision_cutoff_ms:
        raise FamilyBFeatureContractErrorV2(
            "flow closure receipt after D is forbidden"
        )


def _validate_kline_bar(
    value: FamilyBKlineBarV2,
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    if not isinstance(value, FamilyBKlineBarV2):
        raise FamilyBFeatureContractErrorV2(
            "kline_bar must be FamilyBKlineBarV2"
        )
    _validate_row_membership(
        value,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        capture_root_sha256=capture_root_sha256,
        schema_sha256=schema_sha256,
        row_name="kline row",
    )
    _verify_row_hash(
        value.row_payload_sha256,
        "KLINE_BAR",
        _kline_document(value, include_row_hash=False),
    )
    if value.interval_ms != FIVE_MINUTE_MS_V2:
        raise FamilyBFeatureContractErrorV2("kline interval must be exact 5m")
    if (value.bar_open_ms, value.bar_close_ms) != (bar_open_ms, bar_close_ms):
        raise FamilyBFeatureContractErrorV2(
            "kline row does not bind the exact decision bar"
        )
    if not value.closed:
        raise FamilyBFeatureContractErrorV2(
            "intrabar kline is forbidden; closed=true is required"
        )
    if value.event_ms < bar_close_ms:
        raise FamilyBFeatureContractErrorV2(
            "closed kline publication event cannot precede k.T"
        )
    if value.event_ms > value.receipt_ms:
        raise FamilyBFeatureContractErrorV2(
            "kline publication event cannot follow its local receipt"
        )
    if value.receipt_ms > decision_cutoff_ms:
        raise FamilyBFeatureContractErrorV2(
            "kline receipt after D is forbidden"
        )


def _slice_hash(label: str, rows: list[dict[str, object]]) -> str:
    return hashlib.sha256(
        _SLICE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "label": label,
                "rows": rows,
                "schema_version": "r4b_family_b_slice_v2",
            }
        )
    ).hexdigest()


def _flow_only_source_root(
    *,
    promoting_plan_sha256: str,
    normal_flow_capture_root_sha256: str,
    normal_flow_nq_schema_sha256: str,
    normal_flow_slice_sha256: str,
) -> str:
    return hashlib.sha256(
        _FLOW_ONLY_SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "normal_flow_capture_root_sha256": (
                    normal_flow_capture_root_sha256
                ),
                "normal_flow_nq_schema_sha256": normal_flow_nq_schema_sha256,
                "normal_flow_slice_sha256": normal_flow_slice_sha256,
                "promoting_plan_sha256": promoting_plan_sha256,
                "schema_version": "r4b_family_b_flow_only_source_root_v2",
            }
        )
    ).hexdigest()


def _feature_source_root(
    *,
    source_lineage: FamilyBFeatureSourceLineageV2,
    book_slice_sha256: str,
    normal_flow_slice_sha256: str,
    prior_feature_slice_sha256: str,
    kline_slice_sha256: str,
) -> str:
    return hashlib.sha256(
        _SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "book_slice_sha256": book_slice_sha256,
                "lineage_root_sha256": source_lineage.root_sha256,
                "kline_slice_sha256": kline_slice_sha256,
                "normal_flow_slice_sha256": normal_flow_slice_sha256,
                "prior_feature_slice_sha256": prior_feature_slice_sha256,
                "schema_version": "r4b_family_b_feature_source_root_v2",
            }
        )
    ).hexdigest()


def _exit_feature_source_root(
    *,
    source_lineage: FamilyBExitSourceLineageV2,
    normal_flow_slice_sha256: str,
    kline_slice_sha256: str,
) -> str:
    return hashlib.sha256(
        _EXIT_SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "kline_slice_sha256": kline_slice_sha256,
                "lineage_root_sha256": source_lineage.root_sha256,
                "normal_flow_slice_sha256": normal_flow_slice_sha256,
                "schema_version": "r4b_family_b_exit_source_root_v2",
            }
        )
    ).hexdigest()


def _lineage_document(value: FamilyBFeatureSourceLineageV2) -> dict[str, object]:
    return {
        "book_capture_root_sha256": value.book_capture_root_sha256,
        "book_schema_sha256": value.book_schema_sha256,
        "kline_capture_root_sha256": value.kline_capture_root_sha256,
        "kline_schema_sha256": value.kline_schema_sha256,
        "normal_flow_capture_root_sha256": value.normal_flow_capture_root_sha256,
        "normal_flow_nq_schema_sha256": value.normal_flow_nq_schema_sha256,
        "prior_feature_capture_root_sha256": value.prior_feature_capture_root_sha256,
        "prior_feature_schema_sha256": value.prior_feature_schema_sha256,
        "schema_version": "r4b_family_b_source_lineage_v2",
    }


def _exit_lineage_document(
    value: FamilyBExitSourceLineageV2,
) -> dict[str, object]:
    return {
        "kline_capture_root_sha256": value.kline_capture_root_sha256,
        "kline_schema_sha256": value.kline_schema_sha256,
        "normal_flow_capture_root_sha256": value.normal_flow_capture_root_sha256,
        "normal_flow_nq_schema_sha256": value.normal_flow_nq_schema_sha256,
        "schema_version": "r4b_family_b_exit_source_lineage_v2",
    }


def _book_level_document(value: FamilyBBookLevelV2) -> dict[str, object]:
    return {
        "contract_multiplier": str(value.contract_multiplier),
        "price": str(value.price),
        "quantity": str(value.quantity),
        "side": value.side.value,
    }


def _book_state_document(
    value: FamilyBBookStateV2,
    *,
    include_row_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "ask_depth_10bp_quote": str(value.ask_depth_10bp_quote),
        "best_ask": str(value.best_ask),
        "best_bid": str(value.best_bid),
        "bid_depth_10bp_quote": str(value.bid_depth_10bp_quote),
        "capture_root_sha256": value.capture_root_sha256,
        "complete_10bp_band": value.complete_10bp_band,
        "final_update_id": value.final_update_id,
        "first_update_id": value.first_update_id,
        "levels": [_book_level_document(level) for level in value.levels],
        "previous_final_update_id": value.previous_final_update_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "receipt_ms": value.receipt_ms,
        "schema_sha256": value.schema_sha256,
        "source": value.source.value,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
        "transaction_time_ms": value.transaction_time_ms,
        "venue": value.venue.value,
    }
    if include_row_hash:
        document["row_payload_sha256"] = value.row_payload_sha256
    return document


def _normal_flow_document(
    value: FamilyBNormalFlowTradeV2,
    *,
    include_row_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "buyer_maker": value.buyer_maker,
        "capture_root_sha256": value.capture_root_sha256,
        "contract_multiplier": str(value.contract_multiplier),
        "normal_quantity": str(value.normal_quantity),
        "price": str(value.price),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "quantity": str(value.quantity),
        "receipt_ms": value.receipt_ms,
        "schema_sha256": value.schema_sha256,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
        "trade_id": value.trade_id,
        "transaction_time_ms": value.transaction_time_ms,
        "venue": value.venue.value,
    }
    if include_row_hash:
        document["row_payload_sha256"] = value.row_payload_sha256
    return document


def _prior_feature_document(
    value: FamilyBPriorBarFeaturesV2,
    *,
    include_row_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_open_ms": value.bar_open_ms,
        "bar_return": str(value.bar_return),
        "capture_root_sha256": value.capture_root_sha256,
        "flow_imbalance": str(value.flow_imbalance),
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_sha256": value.schema_sha256,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_row_hash:
        document["row_payload_sha256"] = value.row_payload_sha256
    return document


def _flow_closure_document(
    value: FamilyBFlowWindowClosureV2,
    *,
    include_row_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "capture_root_sha256": value.capture_root_sha256,
        "closure_event_ms": value.closure_event_ms,
        "closure_receipt_ms": value.closure_receipt_ms,
        "complete_through_event_ms": value.complete_through_event_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_sha256": value.schema_sha256,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_row_hash:
        document["row_payload_sha256"] = value.row_payload_sha256
    return document


def _flow_only_bar_document(
    value: FamilyBFlowOnlyBarEvidenceV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "flow_imbalance": (
            None if value.flow_imbalance is None else str(value.flow_imbalance)
        ),
        "flow_source_root_sha256": value.flow_source_root_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "normal_flow_capture_root_sha256": (
            value.normal_flow_capture_root_sha256
        ),
        "normal_flow_nq_schema_sha256": value.normal_flow_nq_schema_sha256,
        "normal_flow_slice_sha256": value.normal_flow_slice_sha256,
        "normal_notional": str(value.normal_notional),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "readiness": value.readiness.value,
        "reasons": list(value.reasons),
        "schema_version": "r4b_family_b_flow_only_bar_evidence_v2",
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": None if value.signed_share is None else str(value.signed_share),
        "symbol": value.symbol,
        "total_trade_notional": str(value.total_trade_notional),
        "venue": value.venue.value,
    }


def _kline_document(
    value: FamilyBKlineBarV2,
    *,
    include_row_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "capture_root_sha256": value.capture_root_sha256,
        "close": str(value.close),
        "close_event_id": value.close_event_id,
        "closed": value.closed,
        "event_ms": value.event_ms,
        "high": str(value.high),
        "interval_ms": value.interval_ms,
        "low": str(value.low),
        "open_event_id": value.open_event_id,
        "previous_close": str(value.previous_close),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "receipt_ms": value.receipt_ms,
        "schema_sha256": value.schema_sha256,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_row_hash:
        document["row_payload_sha256"] = value.row_payload_sha256
    return document


def _feature_document(value: FamilyBFeatureEvidenceV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "bar_return_current": str(value.bar_return_current),
        "bar_return_rz_status": value.bar_return_rz_status.value,
        "book_slice_sha256": value.book_slice_sha256,
        "d_end": str(value.d_end),
        "d_low": str(value.d_low),
        "d_start": str(value.d_start),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "feature_source_root_sha256": value.feature_source_root_sha256,
        "flow_imbalance_current": str(value.flow_imbalance_current),
        "flow_rz_status": value.flow_rz_status.value,
        "high_current": str(value.high_current),
        "kline_slice_sha256": value.kline_slice_sha256,
        "kline_event_ms": value.kline_event_ms,
        "kline_evidence_sha256": value.kline_evidence_sha256,
        "kline_receipt_ms": value.kline_receipt_ms,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "low_current": str(value.low_current),
        "normal_flow_slice_sha256": value.normal_flow_slice_sha256,
        "previous_close": str(value.previous_close),
        "prior_feature_slice_sha256": value.prior_feature_slice_sha256,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "readiness": value.readiness.value,
        "rz_bar_return_current": (
            None
            if value.rz_bar_return_current is None
            else str(value.rz_bar_return_current)
        ),
        "rz_flow_imbalance_current": (
            None
            if value.rz_flow_imbalance_current is None
            else str(value.rz_flow_imbalance_current)
        ),
        "schema_version": "r4b_family_b_feature_evidence_v2",
        "source_lineage": _lineage_document(value.source_lineage),
        "spread95_bps": str(value.spread95_bps),
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _exit_feature_document(
    value: FamilyBExitFeatureEvidenceV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close_price": str(value.close_price),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "exit_source_root_sha256": value.exit_source_root_sha256,
        "flow_imbalance_current": str(value.flow_imbalance_current),
        "kline_evidence_sha256": value.kline_evidence_sha256,
        "kline_event_ms": value.kline_event_ms,
        "kline_receipt_ms": value.kline_receipt_ms,
        "kline_slice_sha256": value.kline_slice_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "normal_flow_slice_sha256": value.normal_flow_slice_sha256,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_version": "r4b_family_b_exit_feature_evidence_v2",
        "source_lineage": _exit_lineage_document(value.source_lineage),
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _row_hash(label: str, document: dict[str, object]) -> str:
    return hashlib.sha256(
        _ROW_HASH_DOMAIN
        + canonical_json_line(
            {
                "label": label,
                "row": document,
                "schema_version": "r4b_family_b_raw_row_v2",
            }
        )
    ).hexdigest()


def _verify_row_hash(
    observed: str,
    label: str,
    document: dict[str, object],
) -> None:
    if observed != _row_hash(label, document):
        raise FamilyBFeatureContractErrorV2(
            f"{label} row hash differs from canonical scalar content"
        )


def _order_book_levels(
    values: tuple[FamilyBBookLevelV2, ...],
) -> tuple[FamilyBBookLevelV2, ...]:
    if type(values) is not tuple:
        raise FamilyBFeatureContractErrorV2(
            "levels must be an immutable tuple of raw book levels"
        )
    return tuple(
        sorted(
            values,
            key=lambda value: (
                0 if value.side is FamilyBBookSideV2.BID else 1,
                -value.price
                if value.side is FamilyBBookSideV2.BID
                else value.price,
            ),
        )
    )


def _validate_raw_book_geometry(value: FamilyBBookStateV2) -> None:
    bids = tuple(
        level for level in value.levels if level.side is FamilyBBookSideV2.BID
    )
    asks = tuple(
        level for level in value.levels if level.side is FamilyBBookSideV2.ASK
    )
    if not bids or not asks:
        raise FamilyBFeatureContractErrorV2(
            "raw book state requires both bid and ask levels"
        )
    if max(level.price for level in bids) > min(level.price for level in asks):
        raise FamilyBFeatureContractErrorV2(
            "authoritative standard book cannot be crossed"
        )


def _raw_depth_quote(
    levels: tuple[FamilyBBookLevelV2, ...],
    side: FamilyBBookSideV2,
    boundary: Decimal,
) -> Decimal:
    with localcontext(protocol_decimal_context_v2()):
        return sum(
            (
                value.price * value.quantity * value.contract_multiplier
                for value in levels
                if value.side is side
                and (
                    value.price >= boundary
                    if side is FamilyBBookSideV2.BID
                    else value.price <= boundary
                )
            ),
            Decimal(0),
        )


def _validate_row_identity(
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
) -> None:
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise FamilyBFeatureContractErrorV2(
            "Family B raw rows require USD-M Futures provenance"
        )
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    _validate_sha256(capture_root_sha256, "capture_root_sha256")
    _validate_sha256(schema_sha256, "schema_sha256")


def _validate_row_membership(
    value: (
        FamilyBBookStateV2
        | FamilyBNormalFlowTradeV2
        | FamilyBPriorBarFeaturesV2
        | FamilyBFlowWindowClosureV2
        | FamilyBKlineBarV2
    ),
    *,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    capture_root_sha256: str,
    schema_sha256: str,
    row_name: str,
) -> None:
    observed = (
        value.symbol,
        value.venue,
        value.promoting_plan_sha256,
        value.capture_root_sha256,
        value.schema_sha256,
    )
    expected = (
        symbol,
        venue,
        promoting_plan_sha256,
        capture_root_sha256,
        schema_sha256,
    )
    if observed != expected:
        raise FamilyBFeatureContractErrorV2(
            f"{row_name} is not a verified member of this symbol/plan/capture/schema"
        )


def _validate_bar(bar_open_ms: int, bar_close_ms: int, decision_cutoff_ms: int) -> None:
    try:
        validate_decision_bar_v2(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    except DecisionClockContractErrorV2 as exc:
        raise FamilyBFeatureContractErrorV2(str(exc)) from exc


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise FamilyBFeatureContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise FamilyBFeatureContractErrorV2(
            "symbol must be a normalized USDT symbol"
        )


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise FamilyBFeatureContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FamilyBFeatureContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )


def _validate_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise FamilyBFeatureContractErrorV2(
            f"{field_name} must be a positive integer"
        )
