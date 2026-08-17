from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Final, TypedDict

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    FIVE_MINUTE_MS_V2,
    validate_decision_bar_v2,
)
from signalbot.r4b_v2.protocol.features import (
    ROBUST_Z_PRIOR_WINDOW_V2,
    RobustZStatusV2,
    robust_z_v2,
)

FAMILY_A_ENTRY_PRIOR_BARS_V2: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 13
FAMILY_A_EXIT_PRIOR_BARS_V2: Final = ROBUST_Z_PRIOR_WINDOW_V2
FAMILY_A_OI_STALENESS_MS_V2: Final = 10_000
FAMILY_A_MARK_STALENESS_MS_V2: Final = 2_000

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_TOKEN = object()
_BINDING_ROOT_DOMAIN = b"R4B_FAMILY_A_SOURCE_BINDING_V2\0"
_PRIOR_ROOT_DOMAIN = b"R4B_FAMILY_A_PRIOR_BAR_V2\0"
_FEATURE_ROOT_DOMAIN = b"R4B_FAMILY_A_FEATURE_SOURCE_V2\0"
_ENTRY_EVIDENCE_DOMAIN = b"R4B_FAMILY_A_ENTRY_EVIDENCE_V2\0"
_EXIT_EVIDENCE_DOMAIN = b"R4B_FAMILY_A_EXIT_EVIDENCE_V2\0"
_CONTRACT_MULTIPLIER_DOMAIN = b"R4B_FAMILY_A_CONTRACT_MULTIPLIER_V2\0"
_COMPLETENESS_CONFLICT_DOMAIN = b"R4B_FAMILY_A_COMPLETENESS_CONFLICT_V2\0"


class FamilyAFeatureContractErrorV2(ValueError):
    """Raised when raw Family A evidence violates the frozen V2 contract."""


class FamilyASourceKindV2(StrEnum):
    KLINE = "KLINE"
    OPEN_INTEREST = "OPEN_INTEREST"
    MARK_INDEX_PREDICTED_FUNDING = "MARK_INDEX_PREDICTED_FUNDING"
    NORMAL_FUTURES_FLOW = "NORMAL_FUTURES_FLOW"


class FamilyAFeatureReadinessV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_WARMUP = "FEATURE_NOT_READY_WARMUP"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    FEATURE_NOT_READY_SOURCE = "FEATURE_NOT_READY_SOURCE"
    INCONCLUSIVE_FLOW = "INCONCLUSIVE_FLOW"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID_FEATURE = "DATA_INVALID_FEATURE"


class _ExitEvidenceBaseV2(TypedDict):
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    source_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int


@dataclass(frozen=True, slots=True)
class FamilyASourceBindingV2:
    """Per-record sealed attempt, venue, capture, and schema provenance."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_kind: FamilyASourceKindV2
    capture_root_sha256: str
    schema_sha256: str
    clock_segment_root_sha256: str
    capture_cursor_ingest_seq: int
    capture_cursor_receipt_ms: int
    candidate_set_complete: bool
    _factory_token: InitVar[object] = None
    transport: TransportV2 = field(init=False)
    route_id: str = field(init=False)
    source_locator: str = field(init=False)
    payload_symbol: str = field(init=False)
    plan_symbol: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "source binding must be created by its USD-M route factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAFeatureContractErrorV2(
                "Family A promoting evidence requires USD-M Futures"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        if not isinstance(self.source_kind, FamilyASourceKindV2):
            raise FamilyAFeatureContractErrorV2("source_kind is unsupported")
        _validate_sha256(self.capture_root_sha256, "capture_root_sha256")
        _validate_sha256(self.schema_sha256, "schema_sha256")
        _validate_sha256(
            self.clock_segment_root_sha256,
            "clock_segment_root_sha256",
        )
        _validate_nonnegative_int(
            self.capture_cursor_ingest_seq,
            "capture_cursor_ingest_seq",
        )
        _validate_nonnegative_int(
            self.capture_cursor_receipt_ms,
            "capture_cursor_receipt_ms",
        )
        if self.candidate_set_complete is not True:
            raise FamilyAFeatureContractErrorV2(
                "source binding must fail closed without a complete candidate cursor"
            )
        transport, route_id, locator = _source_route_contract(
            self.source_kind,
            self.symbol,
        )
        object.__setattr__(self, "transport", transport)
        object.__setattr__(self, "route_id", route_id)
        object.__setattr__(self, "source_locator", locator)
        object.__setattr__(self, "payload_symbol", self.symbol)
        object.__setattr__(self, "plan_symbol", self.symbol)

    @classmethod
    def create(
        cls,
        *,
        attempt_id: str,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        source_kind: FamilyASourceKindV2,
        capture_root_sha256: str,
        schema_sha256: str,
        clock_segment_root_sha256: str,
        capture_cursor_ingest_seq: int,
        capture_cursor_receipt_ms: int,
        candidate_set_complete: bool,
    ) -> FamilyASourceBindingV2:
        return cls(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            source_kind=source_kind,
            capture_root_sha256=capture_root_sha256,
            schema_sha256=schema_sha256,
            clock_segment_root_sha256=clock_segment_root_sha256,
            capture_cursor_ingest_seq=capture_cursor_ingest_seq,
            capture_cursor_receipt_ms=capture_cursor_receipt_ms,
            candidate_set_complete=candidate_set_complete,
            _factory_token=_FACTORY_TOKEN,
        )

    @property
    def root_sha256(self) -> str:
        return _hash_document(_BINDING_ROOT_DOMAIN, _binding_document(self))


@dataclass(frozen=True, slots=True)
class FamilyAClosedKlineV2:
    binding: FamilyASourceBindingV2
    bar_open_ms: int
    bar_close_ms: int
    event_time_ms: int
    receipt_ms: int
    close: Decimal
    high: Decimal
    low: Decimal
    source_evidence_sha256: str
    closed: bool = True

    def __post_init__(self) -> None:
        _require_binding(self.binding, FamilyASourceKindV2.KLINE)
        _validate_nonnegative_int(self.bar_open_ms, "bar_open_ms")
        _validate_nonnegative_int(self.bar_close_ms, "bar_close_ms")
        if self.bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyAFeatureContractErrorV2("kline must align to a 5m UTC slot")
        if self.bar_close_ms != self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
            raise FamilyAFeatureContractErrorV2("kline close differs from its 5m slot")
        _validate_nonnegative_int(self.event_time_ms, "event_time_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if self.receipt_ms > self.binding.capture_cursor_receipt_ms:
            raise FamilyAFeatureContractErrorV2("kline receipt exceeds its bound capture cursor")
        if not self.closed:
            raise FamilyAFeatureContractErrorV2("Family A accepts fully closed klines only")
        if not self.bar_open_ms <= self.event_time_ms <= self.bar_close_ms:
            raise FamilyAFeatureContractErrorV2("kline event time must lie inside its closed bar")
        if not all(_is_positive_finite(value) for value in (self.close, self.high, self.low)):
            raise FamilyAFeatureContractErrorV2("kline prices must be positive finite Decimal")
        if self.low > self.close or self.close > self.high:
            raise FamilyAFeatureContractErrorV2("kline low/close/high ordering is invalid")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")


@dataclass(frozen=True, slots=True)
class FamilyAOIResponseV2:
    binding: FamilyASourceBindingV2
    payload_time_ms: int
    response_completion_ms: int
    ingest_seq: int
    open_interest: Decimal
    source_evidence_sha256: str

    def __post_init__(self) -> None:
        _require_binding(self.binding, FamilyASourceKindV2.OPEN_INTEREST)
        _validate_nonnegative_int(self.payload_time_ms, "payload_time_ms")
        _validate_nonnegative_int(self.response_completion_ms, "response_completion_ms")
        _validate_nonnegative_int(self.ingest_seq, "ingest_seq")
        if (
            self.ingest_seq > self.binding.capture_cursor_ingest_seq
            or self.response_completion_ms > self.binding.capture_cursor_receipt_ms
        ):
            raise FamilyAFeatureContractErrorV2("OI response exceeds its bound capture cursor")
        if not _is_positive_finite(self.open_interest):
            raise FamilyAFeatureContractErrorV2("open interest must be positive finite Decimal")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")


@dataclass(frozen=True, slots=True)
class FamilyAMarkIndexFundingV2:
    binding: FamilyASourceBindingV2
    transaction_time_ms: int
    receipt_ms: int
    ingest_seq: int
    mark_price: Decimal
    index_price: Decimal
    predicted_funding_rate: Decimal
    source_evidence_sha256: str

    def __post_init__(self) -> None:
        _require_binding(
            self.binding,
            FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING,
        )
        _validate_nonnegative_int(self.transaction_time_ms, "transaction_time_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        _validate_nonnegative_int(self.ingest_seq, "ingest_seq")
        if (
            self.ingest_seq > self.binding.capture_cursor_ingest_seq
            or self.receipt_ms > self.binding.capture_cursor_receipt_ms
        ):
            raise FamilyAFeatureContractErrorV2("mark observation exceeds its bound capture cursor")
        if not _is_positive_finite(self.mark_price) or not _is_positive_finite(self.index_price):
            raise FamilyAFeatureContractErrorV2(
                "mark and index prices must be positive finite Decimal"
            )
        if not _is_finite_decimal(self.predicted_funding_rate):
            raise FamilyAFeatureContractErrorV2("predicted funding rate must be finite Decimal")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")


@dataclass(frozen=True, slots=True)
class FamilyANormalFlowTradeV2:
    binding: FamilyASourceBindingV2
    trade_id: int
    transaction_time_ms: int
    receipt_ms: int
    price: Decimal
    quantity: Decimal
    normal_quantity: Decimal
    contract_multiplier: Decimal
    buyer_maker: bool
    source_evidence_sha256: str

    def __post_init__(self) -> None:
        _require_binding(self.binding, FamilyASourceKindV2.NORMAL_FUTURES_FLOW)
        _validate_nonnegative_int(self.trade_id, "trade_id")
        _validate_nonnegative_int(self.transaction_time_ms, "transaction_time_ms")
        _validate_nonnegative_int(self.receipt_ms, "receipt_ms")
        if self.receipt_ms > self.binding.capture_cursor_receipt_ms:
            raise FamilyAFeatureContractErrorV2(
                "normal-flow trade receipt exceeds its bound capture cursor"
            )
        if not _is_positive_finite(self.price):
            raise FamilyAFeatureContractErrorV2("trade price must be positive finite Decimal")
        if not _is_nonnegative_finite(self.quantity) or not _is_nonnegative_finite(
            self.normal_quantity
        ):
            raise FamilyAFeatureContractErrorV2("q and nq must be nonnegative finite Decimal")
        if self.normal_quantity > self.quantity:
            raise FamilyAFeatureContractErrorV2("normal quantity nq cannot exceed q")
        if not _is_positive_finite(self.contract_multiplier):
            raise FamilyAFeatureContractErrorV2(
                "contract multiplier must be positive finite Decimal"
            )
        if type(self.buyer_maker) is not bool:
            raise FamilyAFeatureContractErrorV2("buyer_maker must be boolean")
        _validate_sha256(self.source_evidence_sha256, "source_evidence_sha256")


@dataclass(frozen=True, slots=True)
class FamilyAContractMultiplierV2:
    """One factory-sealed symbol/version multiplier valid for a closed interval."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    contract_multiplier: Decimal
    effective_from_ms: int
    effective_until_ms: int
    source_root_sha256: str
    schema_sha256: str
    _factory_token: InitVar[object] = None
    version_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "contract multiplier must be created by its sealed factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAFeatureContractErrorV2("contract multiplier requires USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        if not _is_positive_finite(self.contract_multiplier):
            raise FamilyAFeatureContractErrorV2(
                "contract multiplier must be positive finite Decimal"
            )
        _validate_nonnegative_int(self.effective_from_ms, "effective_from_ms")
        _validate_nonnegative_int(self.effective_until_ms, "effective_until_ms")
        if self.effective_until_ms < self.effective_from_ms:
            raise FamilyAFeatureContractErrorV2("contract multiplier validity interval is inverted")
        _validate_sha256(self.source_root_sha256, "source_root_sha256")
        _validate_sha256(self.schema_sha256, "schema_sha256")
        object.__setattr__(
            self,
            "version_sha256",
            _hash_document(
                _CONTRACT_MULTIPLIER_DOMAIN,
                _contract_multiplier_document(self),
            ),
        )


def build_family_a_contract_multiplier_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    contract_multiplier: Decimal,
    effective_from_ms: int,
    effective_until_ms: int,
    source_root_sha256: str,
    schema_sha256: str,
) -> FamilyAContractMultiplierV2:
    """Seal one exchange-contract version; acquisition remains a capture concern."""

    return FamilyAContractMultiplierV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        contract_multiplier=contract_multiplier,
        effective_from_ms=effective_from_ms,
        effective_until_ms=effective_until_ms,
        source_root_sha256=source_root_sha256,
        schema_sha256=schema_sha256,
        _factory_token=_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class FamilyAFlowWindowV2:
    binding: FamilyASourceBindingV2
    bar_open_ms: int
    bar_close_ms: int
    trades: tuple[FamilyANormalFlowTradeV2, ...]
    contract_version: FamilyAContractMultiplierV2
    capture_complete: bool
    completeness_completion_ms: int
    completeness_evidence_sha256: str

    def __post_init__(self) -> None:
        _require_binding(self.binding, FamilyASourceKindV2.NORMAL_FUTURES_FLOW)
        _validate_bar_slot(self.bar_open_ms, self.bar_close_ms)
        if not isinstance(self.contract_version, FamilyAContractMultiplierV2):
            raise FamilyAFeatureContractErrorV2(
                "flow window requires a sealed contract multiplier version"
            )
        _require_identity(
            self.binding,
            self.contract_version.attempt_id,
            self.contract_version.symbol,
            self.contract_version.venue,
            self.contract_version.promoting_plan_sha256,
        )
        if not (
            self.contract_version.effective_from_ms <= self.bar_open_ms
            and self.bar_close_ms <= self.contract_version.effective_until_ms
        ):
            raise FamilyAFeatureContractErrorV2(
                "one contract multiplier version must cover the full flow window"
            )
        if type(self.trades) is not tuple:
            raise FamilyAFeatureContractErrorV2("flow trades must be an immutable tuple")
        if type(self.capture_complete) is not bool:
            raise FamilyAFeatureContractErrorV2("capture_complete must be boolean")
        _validate_nonnegative_int(
            self.completeness_completion_ms,
            "completeness_completion_ms",
        )
        if self.completeness_completion_ms <= self.bar_close_ms:
            raise FamilyAFeatureContractErrorV2(
                "flow completeness is first observable at T+1 or later"
            )
        if self.completeness_completion_ms > self.binding.capture_cursor_receipt_ms:
            raise FamilyAFeatureContractErrorV2(
                "flow completeness exceeds its bound capture cursor"
            )
        _validate_sha256(
            self.completeness_evidence_sha256,
            "completeness_evidence_sha256",
        )
        identities: dict[int, FamilyANormalFlowTradeV2] = {}
        for trade in self.trades:
            if not isinstance(trade, FamilyANormalFlowTradeV2):
                raise FamilyAFeatureContractErrorV2(
                    "flow window accepts FamilyANormalFlowTradeV2 only"
                )
            _require_same_binding(self.binding, trade.binding)
            if not self.bar_open_ms <= trade.transaction_time_ms <= self.bar_close_ms:
                raise FamilyAFeatureContractErrorV2(
                    "flow trade transaction time lies outside its closed bar"
                )
            if trade.contract_multiplier != self.contract_version.contract_multiplier:
                raise FamilyAFeatureContractErrorV2(
                    "mixed or caller-drifted contract multipliers are forbidden"
                )
            prior = identities.get(trade.trade_id)
            if prior is not None and prior != trade:
                raise FamilyAFeatureContractErrorV2("conflicting normal-flow duplicate trade ID")
            identities[trade.trade_id] = trade
        object.__setattr__(
            self,
            "trades",
            tuple(
                sorted(
                    identities.values(),
                    key=lambda value: (
                        value.transaction_time_ms,
                        value.trade_id,
                        value.receipt_ms,
                        value.source_evidence_sha256,
                    ),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class FamilyACompletenessConflictV2:
    """Post-D evidence that a prior complete-at-D flow window was contradicted."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    declared_complete_ms: int
    completeness_evidence_sha256: str
    late_trade_slice_sha256: str
    late_trade_count: int
    first_late_receipt_ms: int
    latest_late_receipt_ms: int
    _factory_token: InitVar[object] = None
    conflict_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "completeness conflict must be created by its detector"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAFeatureContractErrorV2("completeness conflict must remain USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar_slot(self.bar_open_ms, self.bar_close_ms)
        _validate_nonnegative_int(self.decision_cutoff_ms, "decision_cutoff_ms")
        _validate_nonnegative_int(self.declared_complete_ms, "declared_complete_ms")
        if self.declared_complete_ms > self.decision_cutoff_ms:
            raise FamilyAFeatureContractErrorV2("conflict requires a complete-at-D declaration")
        for value, field_name in (
            (self.completeness_evidence_sha256, "completeness_evidence_sha256"),
            (self.late_trade_slice_sha256, "late_trade_slice_sha256"),
        ):
            _validate_sha256(value, field_name)
        if type(self.late_trade_count) is not int or self.late_trade_count < 1:
            raise FamilyAFeatureContractErrorV2("conflict requires at least one late trade")
        _validate_nonnegative_int(self.first_late_receipt_ms, "first_late_receipt_ms")
        _validate_nonnegative_int(self.latest_late_receipt_ms, "latest_late_receipt_ms")
        if not (
            self.decision_cutoff_ms < self.first_late_receipt_ms <= self.latest_late_receipt_ms
        ):
            raise FamilyAFeatureContractErrorV2("late conflict receipts must lie strictly after D")
        object.__setattr__(
            self,
            "conflict_sha256",
            _hash_document(
                _COMPLETENESS_CONFLICT_DOMAIN,
                _completeness_conflict_document(self),
            ),
        )


def build_family_a_post_cutoff_completeness_conflict_v2(
    window: FamilyAFlowWindowV2,
    *,
    decision_cutoff_ms: int,
) -> FamilyACompletenessConflictV2 | None:
    """Keep late contradictions separate from the immutable fixed-D payload."""

    if not isinstance(window, FamilyAFlowWindowV2):
        raise FamilyAFeatureContractErrorV2("window must be FamilyAFlowWindowV2")
    _validate_nonnegative_int(decision_cutoff_ms, "decision_cutoff_ms")
    if not window.capture_complete or window.completeness_completion_ms > decision_cutoff_ms:
        return None
    late = tuple(value for value in window.trades if value.receipt_ms > decision_cutoff_ms)
    if not late:
        return None
    late_slice = _feature_source_root(
        label="POST_D_COMPLETENESS_CONFLICT",
        rows=[_trade_document(value) for value in late],
    )
    receipts = tuple(value.receipt_ms for value in late)
    binding = window.binding
    return FamilyACompletenessConflictV2(
        attempt_id=binding.attempt_id,
        symbol=binding.symbol,
        venue=binding.venue,
        promoting_plan_sha256=binding.promoting_plan_sha256,
        bar_open_ms=window.bar_open_ms,
        bar_close_ms=window.bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        declared_complete_ms=window.completeness_completion_ms,
        completeness_evidence_sha256=window.completeness_evidence_sha256,
        late_trade_slice_sha256=late_slice,
        late_trade_count=len(late),
        first_late_receipt_ms=min(receipts),
        latest_late_receipt_ms=max(receipts),
        _factory_token=_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class FamilyAPriorBarEvidenceV2:
    """Factory-selected raw bar state evaluated as of one later decision D."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    as_of_cutoff_ms: int
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    kline: FamilyAClosedKlineV2
    selected_oi: FamilyAOIResponseV2
    selected_mark: FamilyAMarkIndexFundingV2
    oi_candidate_slice_sha256: str
    mark_candidate_slice_sha256: str
    source_root_sha256: str
    _factory_token: InitVar[object] = None
    evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "prior-bar evidence must be created by its causal factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAFeatureContractErrorV2("prior evidence must be USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_nonnegative_int(self.as_of_cutoff_ms, "as_of_cutoff_ms")
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        for value, name in (
            (self.oi_candidate_slice_sha256, "oi_candidate_slice_sha256"),
            (self.mark_candidate_slice_sha256, "mark_candidate_slice_sha256"),
            (self.source_root_sha256, "source_root_sha256"),
        ):
            _validate_sha256(value, name)
        bindings = (
            self.kline.binding,
            self.selected_oi.binding,
            self.selected_mark.binding,
        )
        for binding in bindings:
            _require_identity(
                binding,
                self.attempt_id,
                self.symbol,
                self.venue,
                self.promoting_plan_sha256,
            )
        if (
            max(
                self.kline.receipt_ms,
                self.selected_oi.response_completion_ms,
                self.selected_mark.receipt_ms,
            )
            > self.as_of_cutoff_ms
        ):
            raise FamilyAFeatureContractErrorV2(
                "prior evidence contains a record received after its as-of D"
            )
        if self.latest_source_event_ms != max(
            self.kline.event_time_ms,
            self.selected_oi.payload_time_ms,
            self.selected_mark.transaction_time_ms,
        ):
            raise FamilyAFeatureContractErrorV2(
                "prior latest source event differs from selected causal rows"
            )
        if (
            self.latest_source_receipt_ms
            < max(
                self.kline.receipt_ms,
                self.selected_oi.response_completion_ms,
                self.selected_mark.receipt_ms,
            )
            or self.latest_source_receipt_ms > self.as_of_cutoff_ms
        ):
            raise FamilyAFeatureContractErrorV2(
                "prior latest source receipt is outside its causal candidate slice"
            )
        expected_root = _prior_source_root(
            self.kline,
            self.selected_oi,
            self.selected_mark,
            self.oi_candidate_slice_sha256,
            self.mark_candidate_slice_sha256,
        )
        if self.source_root_sha256 != expected_root:
            raise FamilyAFeatureContractErrorV2("prior source root differs from raw records")
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_PRIOR_ROOT_DOMAIN, _prior_document(self)),
        )

    @property
    def bar_open_ms(self) -> int:
        return self.kline.bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self.kline.bar_close_ms

    @property
    def close(self) -> Decimal:
        return self.kline.close

    @property
    def high(self) -> Decimal:
        return self.kline.high

    @property
    def low(self) -> Decimal:
        return self.kline.low

    @property
    def open_interest(self) -> Decimal:
        return self.selected_oi.open_interest

    @property
    def basis(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return (self.selected_mark.mark_price / self.selected_mark.index_price).ln()

    @property
    def funding(self) -> Decimal:
        return self.selected_mark.predicted_funding_rate


@dataclass(frozen=True, slots=True)
class FamilyAEntryFeatureEvidenceV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    source_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    readiness: FamilyAFeatureReadinessV2
    reasons: tuple[str, ...]
    r12_previous: Decimal | None
    rz_r12_previous: Decimal | None
    rz_doi12_previous: Decimal | None
    rz_basis_previous: Decimal | None
    rz_funding_previous: Decimal | None
    rz_r1_current: Decimal | None
    rz_doi1_current: Decimal | None
    flow_current: Decimal | None
    crowded_long_high: Decimal | None
    crowded_short_low: Decimal | None
    _factory_token: InitVar[object] = None
    evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "entry evidence must be created by its causal factory"
            )
        _validate_feature_identity(self)
        _validate_reasons(self.reasons)
        values = _entry_values(self)
        if self.readiness is FamilyAFeatureReadinessV2.READY:
            if any(not _is_finite_decimal(value) for value in values):
                raise FamilyAFeatureContractErrorV2(
                    "READY entry evidence requires all finite scalar features"
                )
            assert self.flow_current is not None
            assert self.crowded_long_high is not None
            assert self.crowded_short_low is not None
            if self.flow_current.copy_abs() > 1:
                raise FamilyAFeatureContractErrorV2("flow imbalance must be in [-1,1]")
            if not _is_positive_finite(self.crowded_long_high) or not _is_positive_finite(
                self.crowded_short_low
            ):
                raise FamilyAFeatureContractErrorV2("crowded references must be positive")
            if self.crowded_short_low > self.crowded_long_high:
                raise FamilyAFeatureContractErrorV2("crowded reference order is invalid")
        elif any(value is not None for value in values):
            raise FamilyAFeatureContractErrorV2(
                "non-ready entry evidence cannot expose partial strategy scalars"
            )
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_ENTRY_EVIDENCE_DOMAIN, _entry_evidence_document(self)),
        )


@dataclass(frozen=True, slots=True)
class FamilyAExitFeatureEvidenceV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    source_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    readiness: FamilyAFeatureReadinessV2
    reasons: tuple[str, ...]
    close_price: Decimal | None
    rz_basis_current: Decimal | None
    flow_previous: Decimal | None
    flow_current: Decimal | None
    _factory_token: InitVar[object] = None
    evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise FamilyAFeatureContractErrorV2(
                "exit evidence must be created by its causal factory"
            )
        _validate_feature_identity(self)
        _validate_reasons(self.reasons)
        core = (self.close_price, self.rz_basis_current)
        if self.readiness is FamilyAFeatureReadinessV2.READY:
            values = (*core, self.flow_previous, self.flow_current)
            if any(not _is_finite_decimal(value) for value in values):
                raise FamilyAFeatureContractErrorV2(
                    "READY exit evidence requires all finite features"
                )
        elif self.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW:
            if any(not _is_finite_decimal(value) for value in core):
                raise FamilyAFeatureContractErrorV2(
                    "flow-inconclusive evidence must retain finite close and basis"
                )
            if self.flow_previous is not None or self.flow_current is not None:
                raise FamilyAFeatureContractErrorV2(
                    "flow-inconclusive evidence cannot expose flow values"
                )
        elif any(value is not None for value in (*core, self.flow_previous, self.flow_current)):
            raise FamilyAFeatureContractErrorV2(
                "non-ready exit evidence cannot expose partial strategy scalars"
            )
        if self.close_price is not None and not _is_positive_finite(self.close_price):
            raise FamilyAFeatureContractErrorV2("exit close must be positive finite")
        for flow in (self.flow_previous, self.flow_current):
            if flow is not None and flow.copy_abs() > 1:
                raise FamilyAFeatureContractErrorV2("flow imbalance must be in [-1,1]")
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_EXIT_EVIDENCE_DOMAIN, _exit_evidence_document(self)),
        )


def build_family_a_prior_bar_evidence_v2(
    *,
    kline: FamilyAClosedKlineV2,
    oi_source_binding: FamilyASourceBindingV2,
    mark_source_binding: FamilyASourceBindingV2,
    oi_responses: tuple[FamilyAOIResponseV2, ...],
    mark_observations: tuple[FamilyAMarkIndexFundingV2, ...],
    as_of_cutoff_ms: int,
) -> FamilyAPriorBarEvidenceV2:
    """Select the latest causal OI and mark rows for one historical bar as of D."""

    if not isinstance(kline, FamilyAClosedKlineV2):
        raise FamilyAFeatureContractErrorV2("kline must be FamilyAClosedKlineV2")
    _validate_nonnegative_int(as_of_cutoff_ms, "as_of_cutoff_ms")
    if kline.receipt_ms > as_of_cutoff_ms:
        raise FamilyAFeatureContractErrorV2("historical kline arrived after as-of D")
    _require_complete_through(kline.binding, as_of_cutoff_ms)
    _require_binding(oi_source_binding, FamilyASourceKindV2.OPEN_INTEREST)
    _require_binding(
        mark_source_binding,
        FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING,
    )
    _require_same_binding(kline.binding, oi_source_binding, source_kind=False)
    _require_same_binding(kline.binding, mark_source_binding, source_kind=False)
    _require_complete_through(oi_source_binding, as_of_cutoff_ms)
    _require_complete_through(mark_source_binding, as_of_cutoff_ms)
    causal_oi = _causal_oi_candidates(
        oi_responses,
        kline.binding,
        cutoff_event_ms=kline.bar_close_ms,
        cutoff_receipt_ms=as_of_cutoff_ms,
    )
    eligible_oi = tuple(
        value
        for value in causal_oi
        if kline.bar_close_ms - value.payload_time_ms <= FAMILY_A_OI_STALENESS_MS_V2
    )
    causal_mark = _causal_mark_candidates(
        mark_observations,
        kline.binding,
        cutoff_event_ms=kline.bar_close_ms,
        cutoff_receipt_ms=as_of_cutoff_ms,
    )
    eligible_mark = tuple(
        value
        for value in causal_mark
        if kline.bar_close_ms - value.transaction_time_ms <= FAMILY_A_MARK_STALENESS_MS_V2
    )
    if not eligible_oi:
        raise FamilyAFeatureContractErrorV2("historical OI is unavailable at 10s bound")
    if not eligible_mark:
        raise FamilyAFeatureContractErrorV2("historical mark/funding is unavailable at 2s bound")
    oi = eligible_oi[-1]
    mark = eligible_mark[-1]
    oi_candidate_slice = _feature_source_root(
        label="PRIOR_OI_CANDIDATES",
        rows=[
            {"candidate_cursor_binding_sha256": oi_source_binding.root_sha256},
            *(_oi_document(value) for value in causal_oi),
        ],
    )
    mark_candidate_slice = _feature_source_root(
        label="PRIOR_MARK_CANDIDATES",
        rows=[
            {"candidate_cursor_binding_sha256": mark_source_binding.root_sha256},
            *(_mark_document(value) for value in causal_mark),
        ],
    )
    source_root = _prior_source_root(
        kline,
        oi,
        mark,
        oi_candidate_slice,
        mark_candidate_slice,
    )
    binding = kline.binding
    return FamilyAPriorBarEvidenceV2(
        attempt_id=binding.attempt_id,
        symbol=binding.symbol,
        venue=binding.venue,
        promoting_plan_sha256=binding.promoting_plan_sha256,
        as_of_cutoff_ms=as_of_cutoff_ms,
        latest_source_event_ms=max(
            kline.event_time_ms,
            oi.payload_time_ms,
            mark.transaction_time_ms,
        ),
        latest_source_receipt_ms=max(
            kline.receipt_ms,
            oi_source_binding.capture_cursor_receipt_ms,
            mark_source_binding.capture_cursor_receipt_ms,
            *(value.response_completion_ms for value in causal_oi),
            *(value.receipt_ms for value in causal_mark),
        ),
        kline=kline,
        selected_oi=oi,
        selected_mark=mark,
        oi_candidate_slice_sha256=oi_candidate_slice,
        mark_candidate_slice_sha256=mark_candidate_slice,
        source_root_sha256=source_root,
        _factory_token=_FACTORY_TOKEN,
    )


def build_family_a_entry_feature_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    current_kline: FamilyAClosedKlineV2,
    current_oi_source_binding: FamilyASourceBindingV2,
    current_oi_responses: tuple[FamilyAOIResponseV2, ...],
    current_flow: FamilyAFlowWindowV2,
    prior_bars: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> FamilyAEntryFeatureEvidenceV2:
    """Build the only scalar entry evidence accepted by Family A."""

    _validate_factory_identity(
        attempt_id,
        symbol,
        venue,
        promoting_plan_sha256,
        bar_open_ms,
        bar_close_ms,
        decision_cutoff_ms,
    )
    expected = (attempt_id, symbol, venue, promoting_plan_sha256)
    _require_raw_identity(current_kline.binding, expected)
    _require_raw_identity(current_oi_source_binding, expected)
    _require_raw_identity(current_flow.binding, expected)
    _require_complete_through(current_kline.binding, decision_cutoff_ms)
    _require_binding(
        current_oi_source_binding,
        FamilyASourceKindV2.OPEN_INTEREST,
    )
    _require_complete_through(current_oi_source_binding, decision_cutoff_ms)
    _require_complete_through(current_flow.binding, decision_cutoff_ms)
    if (current_kline.bar_open_ms, current_kline.bar_close_ms) != (
        bar_open_ms,
        bar_close_ms,
    ) or (current_flow.bar_open_ms, current_flow.bar_close_ms) != (
        bar_open_ms,
        bar_close_ms,
    ):
        raise FamilyAFeatureContractErrorV2("current raw windows differ from decision bar")
    if type(prior_bars) is not tuple:
        raise FamilyAFeatureContractErrorV2("prior_bars must be an immutable tuple")
    if len(prior_bars) > FAMILY_A_ENTRY_PRIOR_BARS_V2:
        raise FamilyAFeatureContractErrorV2("entry prior history exceeds exact window")
    if prior_bars:
        _validate_prior_sequence(
            prior_bars,
            expected=expected,
            expected_first_open_ms=(bar_open_ms - len(prior_bars) * FIVE_MINUTE_MS_V2),
            as_of_cutoff_ms=decision_cutoff_ms,
        )

    causal_oi = _causal_oi_candidates(
        current_oi_responses,
        current_kline.binding,
        cutoff_event_ms=bar_close_ms,
        cutoff_receipt_ms=decision_cutoff_ms,
    )
    eligible_oi = tuple(
        value
        for value in causal_oi
        if bar_close_ms - value.payload_time_ms <= FAMILY_A_OI_STALENESS_MS_V2
    )
    eligible_flow = _eligible_flow(current_flow, decision_cutoff_ms)
    source_root = _feature_source_root(
        label="ENTRY",
        rows=[
            _kline_document(current_kline),
            {"oi_candidate_cursor_binding_sha256": (current_oi_source_binding.root_sha256)},
            *(_oi_document(value) for value in causal_oi),
            _flow_window_document(
                current_flow,
                eligible_flow,
                decision_cutoff_ms=decision_cutoff_ms,
            ),
            *(_prior_reference_document(value) for value in prior_bars),
        ],
    )
    event_times = [
        current_kline.event_time_ms,
        *(value.payload_time_ms for value in eligible_oi),
        *(value.transaction_time_ms for value in eligible_flow),
        *(value.latest_source_event_ms for value in prior_bars),
    ]
    receipt_times = [
        current_kline.receipt_ms,
        *(value.response_completion_ms for value in causal_oi),
        current_oi_source_binding.capture_cursor_receipt_ms,
        *(value.receipt_ms for value in eligible_flow),
        current_flow.completeness_completion_ms,
        *(value.latest_source_receipt_ms for value in prior_bars),
    ]

    nonready = _entry_nonready_reason(
        current_kline=current_kline,
        decision_cutoff_ms=decision_cutoff_ms,
        eligible_oi=eligible_oi,
        current_flow=current_flow,
        prior_bars=prior_bars,
        expected=expected,
        bar_open_ms=bar_open_ms,
    )
    if nonready is not None:
        readiness, reason = nonready
        return _entry_nonready(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
            source_root_sha256=source_root,
            latest_source_event_ms=max(event_times),
            latest_source_receipt_ms=max(receipt_times),
            readiness=readiness,
            reason=reason,
        )

    selected_oi = eligible_oi[-1]
    values = _derive_entry_values(
        current_kline=current_kline,
        current_oi=selected_oi,
        current_flow=eligible_flow,
        prior_bars=prior_bars,
    )
    if (
        isinstance(values, tuple)
        and len(values) == 2
        and isinstance(values[0], FamilyAFeatureReadinessV2)
    ):
        readiness, reason = values
        return _entry_nonready(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
            source_root_sha256=source_root,
            latest_source_event_ms=max(event_times),
            latest_source_receipt_ms=max(receipt_times),
            readiness=readiness,
            reason=reason,
        )
    assert isinstance(values, _EntryValuesV2)
    return FamilyAEntryFeatureEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        source_root_sha256=source_root,
        latest_source_event_ms=max(event_times),
        latest_source_receipt_ms=max(receipt_times),
        readiness=FamilyAFeatureReadinessV2.READY,
        reasons=("EXACT_CAUSAL_ENTRY_FEATURES_READY",),
        r12_previous=values.r12_previous,
        rz_r12_previous=values.rz_r12_previous,
        rz_doi12_previous=values.rz_doi12_previous,
        rz_basis_previous=values.rz_basis_previous,
        rz_funding_previous=values.rz_funding_previous,
        rz_r1_current=values.rz_r1_current,
        rz_doi1_current=values.rz_doi1_current,
        flow_current=values.flow_current,
        crowded_long_high=values.crowded_long_high,
        crowded_short_low=values.crowded_short_low,
        _factory_token=_FACTORY_TOKEN,
    )


def build_family_a_exit_feature_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    current_kline: FamilyAClosedKlineV2,
    current_mark_source_binding: FamilyASourceBindingV2,
    current_mark_observations: tuple[FamilyAMarkIndexFundingV2, ...],
    prior_basis_bars: tuple[FamilyAPriorBarEvidenceV2, ...],
    previous_flow: FamilyAFlowWindowV2,
    current_flow: FamilyAFlowWindowV2,
) -> FamilyAExitFeatureEvidenceV2:
    """Build one closed-bar exit feature set without using future receipts."""

    _validate_factory_identity(
        attempt_id,
        symbol,
        venue,
        promoting_plan_sha256,
        bar_open_ms,
        bar_close_ms,
        decision_cutoff_ms,
    )
    expected = (attempt_id, symbol, venue, promoting_plan_sha256)
    for binding in (
        current_kline.binding,
        current_mark_source_binding,
        previous_flow.binding,
        current_flow.binding,
    ):
        _require_raw_identity(binding, expected)
        _require_complete_through(binding, decision_cutoff_ms)
    _require_binding(
        current_mark_source_binding,
        FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING,
    )
    if (current_kline.bar_open_ms, current_kline.bar_close_ms) != (
        bar_open_ms,
        bar_close_ms,
    ) or (current_flow.bar_open_ms, current_flow.bar_close_ms) != (
        bar_open_ms,
        bar_close_ms,
    ):
        raise FamilyAFeatureContractErrorV2("current exit windows differ from decision bar")
    if previous_flow.bar_open_ms != bar_open_ms - FIVE_MINUTE_MS_V2:
        raise FamilyAFeatureContractErrorV2("previous flow must be exactly bar j-1")
    if type(prior_basis_bars) is not tuple:
        raise FamilyAFeatureContractErrorV2("prior_basis_bars must be an immutable tuple")
    if len(prior_basis_bars) > FAMILY_A_EXIT_PRIOR_BARS_V2:
        raise FamilyAFeatureContractErrorV2("exit basis history exceeds exact window")
    if prior_basis_bars:
        _validate_prior_sequence(
            prior_basis_bars,
            expected=expected,
            expected_first_open_ms=(bar_open_ms - len(prior_basis_bars) * FIVE_MINUTE_MS_V2),
            as_of_cutoff_ms=decision_cutoff_ms,
        )

    causal_mark = _causal_mark_candidates(
        current_mark_observations,
        current_kline.binding,
        cutoff_event_ms=bar_close_ms,
        cutoff_receipt_ms=decision_cutoff_ms,
    )
    eligible_mark = tuple(
        value
        for value in causal_mark
        if bar_close_ms - value.transaction_time_ms <= FAMILY_A_MARK_STALENESS_MS_V2
    )
    previous_trades = _eligible_flow(previous_flow, decision_cutoff_ms)
    current_trades = _eligible_flow(current_flow, decision_cutoff_ms)
    source_root = _feature_source_root(
        label="EXIT",
        rows=[
            _kline_document(current_kline),
            {"mark_candidate_cursor_binding_sha256": (current_mark_source_binding.root_sha256)},
            *(_mark_document(value) for value in causal_mark),
            _flow_window_document(
                previous_flow,
                previous_trades,
                decision_cutoff_ms=decision_cutoff_ms,
            ),
            _flow_window_document(
                current_flow,
                current_trades,
                decision_cutoff_ms=decision_cutoff_ms,
            ),
            *(_prior_reference_document(value) for value in prior_basis_bars),
        ],
    )
    event_times = [
        current_kline.event_time_ms,
        *(value.transaction_time_ms for value in eligible_mark),
        *(value.transaction_time_ms for value in previous_trades),
        *(value.transaction_time_ms for value in current_trades),
    ]
    receipt_times = [
        current_kline.receipt_ms,
        current_mark_source_binding.capture_cursor_receipt_ms,
        *(value.receipt_ms for value in causal_mark),
        *(value.receipt_ms for value in previous_trades),
        *(value.receipt_ms for value in current_trades),
        previous_flow.completeness_completion_ms,
        current_flow.completeness_completion_ms,
        *(value.latest_source_receipt_ms for value in prior_basis_bars),
    ]
    base: _ExitEvidenceBaseV2 = {
        "attempt_id": attempt_id,
        "symbol": symbol,
        "venue": venue,
        "promoting_plan_sha256": promoting_plan_sha256,
        "bar_open_ms": bar_open_ms,
        "bar_close_ms": bar_close_ms,
        "decision_cutoff_ms": decision_cutoff_ms,
        "source_root_sha256": source_root,
        "latest_source_event_ms": max(event_times),
        "latest_source_receipt_ms": max(receipt_times),
    }
    if current_kline.receipt_ms > decision_cutoff_ms:
        return _exit_nonready(
            **base,
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            reason="KLINE_RECEIPT_AFTER_D",
        )
    if len(prior_basis_bars) < FAMILY_A_EXIT_PRIOR_BARS_V2:
        return _exit_nonready(
            **base,
            readiness=FamilyAFeatureReadinessV2.FEATURE_NOT_READY_WARMUP,
            reason="EXACT_8640_BASIS_PRIOR_REQUIRED",
        )
    if not eligible_mark:
        return _exit_nonready(
            **base,
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            reason="MARK_FUNDING_UNAVAILABLE_2000MS",
        )
    selected_mark = eligible_mark[-1]
    with localcontext(protocol_decimal_context_v2()):
        current_basis = (selected_mark.mark_price / selected_mark.index_price).ln()
    basis_rz = robust_z_v2(tuple(value.basis for value in prior_basis_bars), current_basis)
    readiness = _readiness_from_robust_statuses((basis_rz.status,))
    if readiness is not FamilyAFeatureReadinessV2.READY:
        return _exit_nonready(**base, readiness=readiness, reason="BASIS_ROBUST_Z_NOT_READY")
    assert basis_rz.value is not None
    if _flow_is_inconclusive(previous_flow, decision_cutoff_ms) or _flow_is_inconclusive(
        current_flow,
        decision_cutoff_ms,
    ):
        return FamilyAExitFeatureEvidenceV2(
            **base,
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
            reasons=("NORMAL_FLOW_CAPTURE_INCOMPLETE",),
            close_price=current_kline.close,
            rz_basis_current=basis_rz.value,
            flow_previous=None,
            flow_current=None,
            _factory_token=_FACTORY_TOKEN,
        )
    return FamilyAExitFeatureEvidenceV2(
        **base,
        readiness=FamilyAFeatureReadinessV2.READY,
        reasons=("EXACT_CAUSAL_EXIT_FEATURES_READY",),
        close_price=current_kline.close,
        rz_basis_current=basis_rz.value,
        flow_previous=_normal_flow_imbalance(previous_trades),
        flow_current=_normal_flow_imbalance(current_trades),
        _factory_token=_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class _EntryValuesV2:
    r12_previous: Decimal
    rz_r12_previous: Decimal
    rz_doi12_previous: Decimal
    rz_basis_previous: Decimal
    rz_funding_previous: Decimal
    rz_r1_current: Decimal
    rz_doi1_current: Decimal
    flow_current: Decimal
    crowded_long_high: Decimal
    crowded_short_low: Decimal


def _derive_entry_values(
    *,
    current_kline: FamilyAClosedKlineV2,
    current_oi: FamilyAOIResponseV2,
    current_flow: tuple[FamilyANormalFlowTradeV2, ...],
    prior_bars: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> _EntryValuesV2 | tuple[FamilyAFeatureReadinessV2, str]:
    with localcontext(protocol_decimal_context_v2()):
        r12_series = tuple(
            (prior_bars[index].close / prior_bars[index - 12].close).ln()
            for index in range(12, len(prior_bars))
        )
        doi12_series = tuple(
            (prior_bars[index].open_interest / prior_bars[index - 12].open_interest).ln()
            for index in range(12, len(prior_bars))
        )
        basis_series = tuple(value.basis for value in prior_bars[12:])
        funding_series = tuple(value.funding for value in prior_bars[12:])
        r1_prior = tuple(
            (prior_bars[index].close / prior_bars[index - 1].close).ln()
            for index in range(13, len(prior_bars))
        )
        doi1_prior = tuple(
            (prior_bars[index].open_interest / prior_bars[index - 1].open_interest).ln()
            for index in range(13, len(prior_bars))
        )
        current_r1 = (current_kline.close / prior_bars[-1].close).ln()
        current_doi1 = (current_oi.open_interest / prior_bars[-1].open_interest).ln()
    results = (
        robust_z_v2(r12_series[:-1], r12_series[-1]),
        robust_z_v2(doi12_series[:-1], doi12_series[-1]),
        robust_z_v2(basis_series[:-1], basis_series[-1]),
        robust_z_v2(funding_series[:-1], funding_series[-1]),
        robust_z_v2(r1_prior, current_r1),
        robust_z_v2(doi1_prior, current_doi1),
    )
    readiness = _readiness_from_robust_statuses(tuple(value.status for value in results))
    if readiness is not FamilyAFeatureReadinessV2.READY:
        return readiness, "ENTRY_ROBUST_Z_NOT_READY"
    rz_values: list[Decimal] = []
    for result in results:
        assert result.value is not None
        rz_values.append(result.value)
    prior_12 = prior_bars[-12:]
    return _EntryValuesV2(
        r12_previous=r12_series[-1],
        rz_r12_previous=rz_values[0],
        rz_doi12_previous=rz_values[1],
        rz_basis_previous=rz_values[2],
        rz_funding_previous=rz_values[3],
        rz_r1_current=rz_values[4],
        rz_doi1_current=rz_values[5],
        flow_current=_normal_flow_imbalance(current_flow),
        crowded_long_high=max(value.high for value in prior_12),
        crowded_short_low=min(value.low for value in prior_12),
    )


def _entry_nonready_reason(
    *,
    current_kline: FamilyAClosedKlineV2,
    decision_cutoff_ms: int,
    eligible_oi: tuple[FamilyAOIResponseV2, ...],
    current_flow: FamilyAFlowWindowV2,
    prior_bars: tuple[FamilyAPriorBarEvidenceV2, ...],
    expected: tuple[str, str, VenueV2, str],
    bar_open_ms: int,
) -> tuple[FamilyAFeatureReadinessV2, str] | None:
    if current_kline.receipt_ms > decision_cutoff_ms:
        return FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA, "KLINE_RECEIPT_AFTER_D"
    if len(prior_bars) < FAMILY_A_ENTRY_PRIOR_BARS_V2:
        return (
            FamilyAFeatureReadinessV2.FEATURE_NOT_READY_WARMUP,
            "EXACT_ENTRY_PRIOR_HISTORY_REQUIRED",
        )
    if not eligible_oi:
        return (
            FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            "CURRENT_OI_UNAVAILABLE_10000MS",
        )
    if _flow_is_inconclusive(current_flow, decision_cutoff_ms):
        return (
            FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
            "CURRENT_NORMAL_FLOW_CAPTURE_INCOMPLETE",
        )
    return None


def _entry_nonready(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    source_root_sha256: str,
    latest_source_event_ms: int,
    latest_source_receipt_ms: int,
    readiness: FamilyAFeatureReadinessV2,
    reason: str,
) -> FamilyAEntryFeatureEvidenceV2:
    return FamilyAEntryFeatureEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        source_root_sha256=source_root_sha256,
        latest_source_event_ms=latest_source_event_ms,
        latest_source_receipt_ms=latest_source_receipt_ms,
        readiness=readiness,
        reasons=(reason,),
        r12_previous=None,
        rz_r12_previous=None,
        rz_doi12_previous=None,
        rz_basis_previous=None,
        rz_funding_previous=None,
        rz_r1_current=None,
        rz_doi1_current=None,
        flow_current=None,
        crowded_long_high=None,
        crowded_short_low=None,
        _factory_token=_FACTORY_TOKEN,
    )


def _exit_nonready(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    source_root_sha256: str,
    latest_source_event_ms: int,
    latest_source_receipt_ms: int,
    readiness: FamilyAFeatureReadinessV2,
    reason: str,
) -> FamilyAExitFeatureEvidenceV2:
    return FamilyAExitFeatureEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        source_root_sha256=source_root_sha256,
        latest_source_event_ms=latest_source_event_ms,
        latest_source_receipt_ms=latest_source_receipt_ms,
        readiness=readiness,
        reasons=(reason,),
        close_price=None,
        rz_basis_current=None,
        flow_previous=None,
        flow_current=None,
        _factory_token=_FACTORY_TOKEN,
    )


def _causal_oi_candidates(
    values: tuple[FamilyAOIResponseV2, ...],
    expected_binding: FamilyASourceBindingV2,
    *,
    cutoff_event_ms: int,
    cutoff_receipt_ms: int,
) -> tuple[FamilyAOIResponseV2, ...]:
    if type(values) is not tuple:
        raise FamilyAFeatureContractErrorV2("OI responses must be an immutable tuple")
    candidates: dict[tuple[int, int, int], FamilyAOIResponseV2] = {}
    for value in values:
        if not isinstance(value, FamilyAOIResponseV2):
            raise FamilyAFeatureContractErrorV2("OI collection contains wrong record type")
        _require_same_binding(expected_binding, value.binding, source_kind=False)
        key = (value.payload_time_ms, value.response_completion_ms, value.ingest_seq)
        prior = candidates.get(key)
        if prior is not None and prior != value:
            raise FamilyAFeatureContractErrorV2("conflicting OI duplicate identity")
        if (
            cutoff_event_ms - FAMILY_A_OI_STALENESS_MS_V2 <= value.payload_time_ms
            and value.payload_time_ms <= cutoff_event_ms
            and value.response_completion_ms <= cutoff_receipt_ms
        ):
            candidates[key] = value
    return tuple(
        sorted(
            candidates.values(),
            key=lambda value: (
                value.payload_time_ms,
                value.response_completion_ms,
                value.ingest_seq,
            ),
        )
    )


def _causal_mark_candidates(
    values: tuple[FamilyAMarkIndexFundingV2, ...],
    expected_binding: FamilyASourceBindingV2,
    *,
    cutoff_event_ms: int,
    cutoff_receipt_ms: int,
) -> tuple[FamilyAMarkIndexFundingV2, ...]:
    if type(values) is not tuple:
        raise FamilyAFeatureContractErrorV2("mark observations must be an immutable tuple")
    candidates: dict[tuple[int, int, int], FamilyAMarkIndexFundingV2] = {}
    for value in values:
        if not isinstance(value, FamilyAMarkIndexFundingV2):
            raise FamilyAFeatureContractErrorV2("mark collection has wrong record type")
        _require_same_binding(expected_binding, value.binding, source_kind=False)
        key = (value.transaction_time_ms, value.receipt_ms, value.ingest_seq)
        prior = candidates.get(key)
        if prior is not None and prior != value:
            raise FamilyAFeatureContractErrorV2("conflicting mark duplicate identity")
        if (
            cutoff_event_ms - FAMILY_A_MARK_STALENESS_MS_V2 <= value.transaction_time_ms
            and value.transaction_time_ms <= cutoff_event_ms
            and value.receipt_ms <= cutoff_receipt_ms
        ):
            candidates[key] = value
    return tuple(
        sorted(
            candidates.values(),
            key=lambda value: (
                value.transaction_time_ms,
                value.receipt_ms,
                value.ingest_seq,
            ),
        )
    )


def _eligible_flow(
    window: FamilyAFlowWindowV2,
    decision_cutoff_ms: int,
) -> tuple[FamilyANormalFlowTradeV2, ...]:
    return tuple(
        sorted(
            (value for value in window.trades if value.receipt_ms <= decision_cutoff_ms),
            key=lambda value: (
                value.transaction_time_ms,
                value.trade_id,
                value.source_evidence_sha256,
            ),
        )
    )


def _flow_is_inconclusive(
    window: FamilyAFlowWindowV2,
    decision_cutoff_ms: int,
) -> bool:
    return not window.capture_complete or window.completeness_completion_ms > decision_cutoff_ms


def _normal_flow_imbalance(
    trades: tuple[FamilyANormalFlowTradeV2, ...],
) -> Decimal:
    with localcontext(protocol_decimal_context_v2()):
        buy = sum(
            (
                value.price * value.normal_quantity * value.contract_multiplier
                for value in trades
                if not value.buyer_maker
            ),
            Decimal(0),
        )
        sell = sum(
            (
                value.price * value.normal_quantity * value.contract_multiplier
                for value in trades
                if value.buyer_maker
            ),
            Decimal(0),
        )
        if buy == 0 and sell == 0:
            return Decimal(0)
        return (buy - sell) / (buy + sell)


def _readiness_from_robust_statuses(
    statuses: tuple[RobustZStatusV2, ...],
) -> FamilyAFeatureReadinessV2:
    if any(value is RobustZStatusV2.DATA_INVALID_FEATURE for value in statuses):
        return FamilyAFeatureReadinessV2.DATA_INVALID_FEATURE
    if any(value is RobustZStatusV2.FEATURE_NOT_READY_WARMUP for value in statuses):
        return FamilyAFeatureReadinessV2.FEATURE_NOT_READY_WARMUP
    if any(value is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE for value in statuses):
        return FamilyAFeatureReadinessV2.FEATURE_NOT_READY_ZERO_SCALE
    return FamilyAFeatureReadinessV2.READY


def _validate_prior_sequence(
    values: tuple[FamilyAPriorBarEvidenceV2, ...],
    *,
    expected: tuple[str, str, VenueV2, str],
    expected_first_open_ms: int,
    as_of_cutoff_ms: int,
) -> None:
    for index, value in enumerate(values):
        if not isinstance(value, FamilyAPriorBarEvidenceV2):
            raise FamilyAFeatureContractErrorV2("prior history has wrong evidence type")
        identity = (
            value.attempt_id,
            value.symbol,
            value.venue,
            value.promoting_plan_sha256,
        )
        if identity != expected:
            raise FamilyAFeatureContractErrorV2("prior evidence identity mismatch")
        if value.as_of_cutoff_ms != as_of_cutoff_ms:
            raise FamilyAFeatureContractErrorV2(
                "prior evidence must use the current decision D as its as-of barrier"
            )
        expected_open = expected_first_open_ms + index * FIVE_MINUTE_MS_V2
        if value.bar_open_ms != expected_open:
            raise FamilyAFeatureContractErrorV2(
                "prior history must be fully contiguous with no survivor dropping"
            )


def _validate_factory_identity(
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    _validate_identity(attempt_id, "attempt_id")
    _validate_symbol(symbol)
    if venue is not VenueV2.USDM_FUTURES:
        raise FamilyAFeatureContractErrorV2("Family A accepts USD-M Futures only")
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    try:
        validate_decision_bar_v2(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    except ValueError as error:
        raise FamilyAFeatureContractErrorV2(str(error)) from error


def _validate_feature_identity(
    value: FamilyAEntryFeatureEvidenceV2 | FamilyAExitFeatureEvidenceV2,
) -> None:
    _validate_factory_identity(
        value.attempt_id,
        value.symbol,
        value.venue,
        value.promoting_plan_sha256,
        value.bar_open_ms,
        value.bar_close_ms,
        value.decision_cutoff_ms,
    )
    _validate_sha256(value.source_root_sha256, "source_root_sha256")
    _validate_nonnegative_int(value.latest_source_event_ms, "latest_source_event_ms")
    _validate_nonnegative_int(value.latest_source_receipt_ms, "latest_source_receipt_ms")
    if value.latest_source_event_ms > value.bar_close_ms:
        raise FamilyAFeatureContractErrorV2("future source event entered feature evidence")
    if not isinstance(value.readiness, FamilyAFeatureReadinessV2):
        raise FamilyAFeatureContractErrorV2("unsupported feature readiness")
    if value.latest_source_receipt_ms > value.decision_cutoff_ms and value.readiness not in (
        FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
        FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
    ):
        raise FamilyAFeatureContractErrorV2("after-D receipt requires INCONCLUSIVE_DATA evidence")


def _entry_values(value: FamilyAEntryFeatureEvidenceV2) -> tuple[Decimal | None, ...]:
    return (
        value.r12_previous,
        value.rz_r12_previous,
        value.rz_doi12_previous,
        value.rz_basis_previous,
        value.rz_funding_previous,
        value.rz_r1_current,
        value.rz_doi1_current,
        value.flow_current,
        value.crowded_long_high,
        value.crowded_short_low,
    )


def _prior_source_root(
    kline: FamilyAClosedKlineV2,
    oi: FamilyAOIResponseV2,
    mark: FamilyAMarkIndexFundingV2,
    oi_candidate_slice_sha256: str,
    mark_candidate_slice_sha256: str,
) -> str:
    return _feature_source_root(
        label="PRIOR_BAR",
        rows=[
            _kline_document(kline),
            _oi_document(oi),
            _mark_document(mark),
            {
                "mark_candidate_slice_sha256": mark_candidate_slice_sha256,
                "oi_candidate_slice_sha256": oi_candidate_slice_sha256,
            },
        ],
    )


def _feature_source_root(*, label: str, rows: list[dict[str, object]]) -> str:
    return _hash_document(
        _FEATURE_ROOT_DOMAIN,
        {
            "label": label,
            "rows": rows,
            "schema_version": "r4b_family_a_feature_source_v2",
        },
    )


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _binding_document(value: FamilyASourceBindingV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "candidate_set_complete": value.candidate_set_complete,
        "capture_root_sha256": value.capture_root_sha256,
        "capture_cursor_ingest_seq": value.capture_cursor_ingest_seq,
        "capture_cursor_receipt_ms": value.capture_cursor_receipt_ms,
        "clock_segment_root_sha256": value.clock_segment_root_sha256,
        "payload_symbol": value.payload_symbol,
        "plan_symbol": value.plan_symbol,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "route_id": value.route_id,
        "schema_sha256": value.schema_sha256,
        "source_locator": value.source_locator,
        "source_kind": value.source_kind.value,
        "symbol": value.symbol,
        "transport": value.transport.value,
        "venue": value.venue.value,
    }


def _kline_document(value: FamilyAClosedKlineV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "binding_root_sha256": value.binding.root_sha256,
        "close": str(value.close),
        "event_time_ms": value.event_time_ms,
        "high": str(value.high),
        "low": str(value.low),
        "receipt_ms": value.receipt_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
    }


def _oi_document(value: FamilyAOIResponseV2) -> dict[str, object]:
    return {
        "binding_root_sha256": value.binding.root_sha256,
        "ingest_seq": value.ingest_seq,
        "open_interest": str(value.open_interest),
        "payload_time_ms": value.payload_time_ms,
        "response_completion_ms": value.response_completion_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
    }


def _mark_document(value: FamilyAMarkIndexFundingV2) -> dict[str, object]:
    return {
        "binding_root_sha256": value.binding.root_sha256,
        "index_price": str(value.index_price),
        "ingest_seq": value.ingest_seq,
        "mark_price": str(value.mark_price),
        "predicted_funding_rate": str(value.predicted_funding_rate),
        "receipt_ms": value.receipt_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
        "transaction_time_ms": value.transaction_time_ms,
    }


def _trade_document(value: FamilyANormalFlowTradeV2) -> dict[str, object]:
    return {
        "binding_root_sha256": value.binding.root_sha256,
        "buyer_maker": value.buyer_maker,
        "contract_multiplier": str(value.contract_multiplier),
        "normal_quantity": str(value.normal_quantity),
        "price": str(value.price),
        "quantity": str(value.quantity),
        "receipt_ms": value.receipt_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
        "trade_id": value.trade_id,
        "transaction_time_ms": value.transaction_time_ms,
    }


def _contract_multiplier_document(
    value: FamilyAContractMultiplierV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "contract_multiplier": str(value.contract_multiplier),
        "effective_from_ms": value.effective_from_ms,
        "effective_until_ms": value.effective_until_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "schema_sha256": value.schema_sha256,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _completeness_conflict_document(
    value: FamilyACompletenessConflictV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "completeness_evidence_sha256": value.completeness_evidence_sha256,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "declared_complete_ms": value.declared_complete_ms,
        "first_late_receipt_ms": value.first_late_receipt_ms,
        "late_trade_count": value.late_trade_count,
        "late_trade_slice_sha256": value.late_trade_slice_sha256,
        "latest_late_receipt_ms": value.latest_late_receipt_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _flow_window_document(
    value: FamilyAFlowWindowV2,
    eligible: tuple[FamilyANormalFlowTradeV2, ...],
    *,
    decision_cutoff_ms: int,
) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "binding_root_sha256": value.binding.root_sha256,
        "capture_complete": value.capture_complete,
        "completeness_completion_ms": value.completeness_completion_ms,
        "completeness_evidence_sha256": value.completeness_evidence_sha256,
        "contract_multiplier": str(value.contract_version.contract_multiplier),
        "contract_version_sha256": value.contract_version.version_sha256,
        "decision_cutoff_ms": decision_cutoff_ms,
        "trades": [_trade_document(item) for item in eligible],
    }


def _prior_reference_document(value: FamilyAPriorBarEvidenceV2) -> dict[str, object]:
    return {
        "as_of_cutoff_ms": value.as_of_cutoff_ms,
        "bar_open_ms": value.bar_open_ms,
        "evidence_sha256": value.evidence_sha256,
        "source_root_sha256": value.source_root_sha256,
    }


def _prior_document(value: FamilyAPriorBarEvidenceV2) -> dict[str, object]:
    return {
        "as_of_cutoff_ms": value.as_of_cutoff_ms,
        "attempt_id": value.attempt_id,
        "kline": _kline_document(value.kline),
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "mark_candidate_slice_sha256": value.mark_candidate_slice_sha256,
        "oi_candidate_slice_sha256": value.oi_candidate_slice_sha256,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "selected_mark": _mark_document(value.selected_mark),
        "selected_oi": _oi_document(value.selected_oi),
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _entry_evidence_document(value: FamilyAEntryFeatureEvidenceV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "crowded_long_high": _decimal_or_none(value.crowded_long_high),
        "crowded_short_low": _decimal_or_none(value.crowded_short_low),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "flow_current": _decimal_or_none(value.flow_current),
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "r12_previous": _decimal_or_none(value.r12_previous),
        "readiness": value.readiness.value,
        "reasons": list(value.reasons),
        "rz_basis_previous": _decimal_or_none(value.rz_basis_previous),
        "rz_doi12_previous": _decimal_or_none(value.rz_doi12_previous),
        "rz_doi1_current": _decimal_or_none(value.rz_doi1_current),
        "rz_funding_previous": _decimal_or_none(value.rz_funding_previous),
        "rz_r12_previous": _decimal_or_none(value.rz_r12_previous),
        "rz_r1_current": _decimal_or_none(value.rz_r1_current),
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _exit_evidence_document(value: FamilyAExitFeatureEvidenceV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close_price": _decimal_or_none(value.close_price),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "flow_current": _decimal_or_none(value.flow_current),
        "flow_previous": _decimal_or_none(value.flow_previous),
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "readiness": value.readiness.value,
        "reasons": list(value.reasons),
        "rz_basis_current": _decimal_or_none(value.rz_basis_current),
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _require_binding(
    value: FamilyASourceBindingV2,
    expected_kind: FamilyASourceKindV2,
) -> None:
    if not isinstance(value, FamilyASourceBindingV2):
        raise FamilyAFeatureContractErrorV2("record binding has wrong type")
    if value.source_kind is not expected_kind:
        raise FamilyAFeatureContractErrorV2(f"record requires {expected_kind.value} source binding")


def _require_same_binding(
    expected: FamilyASourceBindingV2,
    actual: FamilyASourceBindingV2,
    *,
    source_kind: bool = True,
) -> None:
    fields_equal = (
        expected.attempt_id == actual.attempt_id
        and expected.symbol == actual.symbol
        and expected.venue is actual.venue
        and expected.promoting_plan_sha256 == actual.promoting_plan_sha256
    )
    if source_kind:
        fields_equal = fields_equal and expected.source_kind is actual.source_kind
    if not fields_equal:
        raise FamilyAFeatureContractErrorV2("raw record binding identity mismatch")


def _require_identity(
    binding: FamilyASourceBindingV2,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
) -> None:
    if (
        binding.attempt_id,
        binding.symbol,
        binding.venue,
        binding.promoting_plan_sha256,
    ) != (attempt_id, symbol, venue, promoting_plan_sha256):
        raise FamilyAFeatureContractErrorV2("source binding differs from feature identity")


def _require_raw_identity(
    binding: FamilyASourceBindingV2,
    expected: tuple[str, str, VenueV2, str],
) -> None:
    _require_identity(binding, *expected)


def _require_complete_through(
    binding: FamilyASourceBindingV2,
    decision_cutoff_ms: int,
) -> None:
    if not binding.candidate_set_complete or binding.capture_cursor_receipt_ms < decision_cutoff_ms:
        raise FamilyAFeatureContractErrorV2(
            "source cursor does not prove candidate completeness through D"
        )


def _validate_bar_slot(bar_open_ms: int, bar_close_ms: int) -> None:
    _validate_nonnegative_int(bar_open_ms, "bar_open_ms")
    _validate_nonnegative_int(bar_close_ms, "bar_close_ms")
    if bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise FamilyAFeatureContractErrorV2("bar must align to a 5m UTC slot")
    if bar_close_ms != bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
        raise FamilyAFeatureContractErrorV2("bar close differs from its 5m slot")


def _validate_reasons(value: tuple[str, ...]) -> None:
    if type(value) is not tuple or not value:
        raise FamilyAFeatureContractErrorV2("reasons must be a non-empty tuple")
    for item in value:
        _validate_identity(item, "reason")


def _validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 256:
        raise FamilyAFeatureContractErrorV2(f"{field_name} must be a bounded normalized identity")


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise FamilyAFeatureContractErrorV2("symbol must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise FamilyAFeatureContractErrorV2(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FamilyAFeatureContractErrorV2(f"{field_name} must be a nonnegative integer")


def _source_route_contract(
    source_kind: FamilyASourceKindV2,
    symbol: str,
) -> tuple[TransportV2, str, str]:
    stream_symbol = symbol.casefold()
    if source_kind is FamilyASourceKindV2.KLINE:
        return TransportV2.WEBSOCKET, "usdm_market", f"{stream_symbol}@kline_5m"
    if source_kind is FamilyASourceKindV2.OPEN_INTEREST:
        return TransportV2.HTTPS, "usdm_public_rest", "/fapi/v1/openInterest"
    if source_kind is FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING:
        return TransportV2.WEBSOCKET, "usdm_market", f"{stream_symbol}@markPrice@1s"
    if source_kind is FamilyASourceKindV2.NORMAL_FUTURES_FLOW:
        return TransportV2.WEBSOCKET, "usdm_market", f"{stream_symbol}@aggTrade"
    raise FamilyAFeatureContractErrorV2("unsupported Family A source route")


def _is_finite_decimal(value: Decimal | None) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: Decimal | None) -> bool:
    return _is_finite_decimal(value) and value is not None and value > 0


def _is_nonnegative_finite(value: Decimal | None) -> bool:
    return _is_finite_decimal(value) and value is not None and value >= 0
