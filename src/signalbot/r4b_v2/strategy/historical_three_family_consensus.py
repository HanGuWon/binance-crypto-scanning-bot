"""Frozen historical-only consensus across three independent directional families.

This sibling module deliberately does not alter the live six-family panel or the
failed Indicator Discriminator V1A score.  It binds one authenticated historical
recommendation anchor to exact price, participation, and target-excluded
cross-sectional proxy documents.  Its output is an uncalibrated descriptive
state, never a probability, promotion decision, or order instruction.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import ROUND_HALF_UP, Decimal, DecimalException, localcontext
from enum import StrEnum
from typing import Final, Literal, cast

from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import RobustZStatusV2
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2,
    HistoricalCrossSectional7AssetCalculationV2,
    HistoricalCrossSectional7AssetProxyContractErrorV2,
    HistoricalCrossSectional7AssetProxyStatusV2,
    HistoricalCrossSectional7AssetProxyV2,
    canonical_historical_cross_sectional_7asset_calculation_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.participation_evidence import (
    PARTICIPATION_FLOW_RULE_VERSION_V2,
    ParticipationFlowCalculationV2,
    ParticipationFlowContractErrorV2,
    ParticipationFlowStatusV2,
    canonical_participation_flow_calculation_v2,
)
from signalbot.r4b_v2.strategy.participation_historical_kline_proxy import (
    ParticipationHistoricalKlineProxyStatusV2,
    ParticipationHistoricalKlineProxyV2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    PriceClosePathCalculationV2,
    PriceEvidenceContractErrorV2,
    canonical_price_close_path_calculation_v2,
)
from signalbot.r4b_v2.strategy.price_historical_kline_proxy import (
    PriceHistoricalKlineProxyStatusV2,
    PriceHistoricalKlineProxyV2,
)

HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_CONSENSUS_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_CONSENSUS_ROLE_V2: Final = (
    "HISTORICAL_ONLY_NONPROMOTING_THREE_FAMILY_DIRECTIONAL_CONSENSUS"
)
HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2: Final = (
    "alert_replay_v3_2026-07-20_indicator_discriminator"
)
HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2: Final = (
    "v4.3.0-causal-structure-diagnostics"
)
HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2: Final = (
    "1000BONKUSDT",
    "ENAUSDT",
    "WIFUSDT",
    "1000FLOKIUSDT",
    "ARBUSDT",
    "OPUSDT",
    "SEIUSDT",
)

_SCHEMA_VERSION: Final = "r4b_historical_three_family_consensus_v2"
_ANCHOR_SCHEMA_VERSION: Final = "r4b_historical_recommendation_anchor_v2"
_LEAF_SCHEMA_VERSION: Final = "r4b_historical_directional_leaf_v2"
_EXECUTION_SCHEMA_VERSION: Final = "r4b_historical_execution_contract_v2"
_COST_SCHEMA_VERSION: Final = "r4b_historical_cost_survival_context_v2"
_EXECUTION_RULE_VERSION: Final = (
    "R4B_HISTORICAL_USDM_5M_FIXED_5BP_FEE_8BP_SLIPPAGE_EACH_SIDE_V1"
)
_ANCHOR_DOMAIN: Final = b"R4B_HISTORICAL_RECOMMENDATION_ANCHOR_V2\0"
_LEAF_DOMAIN: Final = b"R4B_HISTORICAL_DIRECTIONAL_LEAF_V2\0"
_EXECUTION_DOMAIN: Final = b"R4B_HISTORICAL_EXECUTION_CONTRACT_V2\0"
_COST_DOMAIN: Final = b"R4B_HISTORICAL_COST_SURVIVAL_CONTEXT_V2\0"
_PEER_ROOT_DOMAIN: Final = b"R4B_HISTORICAL_CONSENSUS_PEER_ROOT_V2\0"
_EVENT_ID_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_CONSENSUS_ID_V2\0"
_PAYLOAD_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_CONSENSUS_PAYLOAD_V2\0"
_LEAF_FACTORY_TOKEN: Final = object()
_COST_FACTORY_TOKEN: Final = object()
_AGGREGATION_FACTORY_TOKEN: Final = object()
_CONSENSUS_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_EVENT_ID_RE: Final = re.compile(r"^[0-9a-f]{24}$")
_ASSET_RE: Final = re.compile(r"^[A-Z0-9]{2,20}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_REASON_RE: Final = re.compile(r"^[A-Z0-9_]+$")
_MAX_CANONICAL_INTEGER: Final = 2**53 - 1
_STRENGTH_SCALE: Final = 1_000_000
_BPS_TO_RETURN_MICROS: Final = 100
_ROUND_TRIP_SIDES: Final = 2
_FROZEN_FEE_BPS_PER_SIDE: Final = 5
_FROZEN_SLIPPAGE_BPS_PER_SIDE: Final = 8
_FROZEN_ZERO_MOVE_COST_MICROS: Final = 2_600
_FIXED_INVALIDATION: Final = (
    "HISTORICAL_CONSENSUS_CANNOT_OPEN_CLOSE_RANK_PROMOTE_OR_PLACE_ORDERS"
)
_SOURCE_AUTHORITY_STATUS: Final = (
    "AUTHENTICATED_V1A_AMENDMENT_ANCHOR_HISTORICAL_CALCULATIONS_ONLY"
)
_ALLOWED_SPLITS: Final = frozenset(
    {"development", "validation", "retrospective_test"}
)
_ASSET_BY_SYMBOL: Final = {
    "1000BONKUSDT": "BONK",
    "ENAUSDT": "ENA",
    "WIFUSDT": "WIF",
    "1000FLOKIUSDT": "FLOKI",
    "ARBUSDT": "ARB",
    "OPUSDT": "OP",
    "SEIUSDT": "SEI",
}
_FAMILY_ORDER: Final = (
    # This is the only order used by arithmetic and canonical payloads.
    "PRICE_STRUCTURE_MOMENTUM",
    "PARTICIPATION_FLOW",
    "CROSS_SECTIONAL_CONTEXT_EX_TARGET",
)


class HistoricalThreeFamilyConsensusContractErrorV2(ValueError):
    """Raised when the frozen historical consensus contract is violated."""


class HistoricalFamilyV2(StrEnum):
    PRICE_STRUCTURE_MOMENTUM = "PRICE_STRUCTURE_MOMENTUM"
    PARTICIPATION_FLOW = "PARTICIPATION_FLOW"
    CROSS_SECTIONAL_CONTEXT_EX_TARGET = "CROSS_SECTIONAL_CONTEXT_EX_TARGET"


class HistoricalLeafStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY = "FEATURE_NOT_READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"


class HistoricalLeafSourceRepresentationV2(StrEnum):
    CANONICAL_NUMERIC_CALCULATION = "CANONICAL_NUMERIC_CALCULATION"
    SEALED_PROXY_UNAVAILABLE = "SEALED_PROXY_UNAVAILABLE"


class HistoricalConsensusStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY = "FEATURE_NOT_READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"


class PrimaryRelationshipV2(StrEnum):
    SUPPORTS_PRIMARY = "SUPPORTS_PRIMARY"
    OPPOSES_PRIMARY = "OPPOSES_PRIMARY"
    MIXED_OR_NEUTRAL = "MIXED_OR_NEUTRAL"
    WITHHELD = "WITHHELD"


class HistoricalCostSurvivalStatusV2(StrEnum):
    AVAILABLE = "AVAILABLE"
    NOT_READY = "NOT_READY"


class HistoricalConsensusRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class HistoricalRecommendationAnchorV2:
    """One exact Futures row from the frozen V1A recommendation population."""

    source_event_id: str
    source_row_sha256: str
    source_replay_manifest_sha256: str
    split: str
    asset: str
    cohort: str
    symbol: str
    primary_family: SignalFamily
    primary_direction: Direction
    decision_time_ms: int
    price: Decimal
    invalidation: Decimal | None
    atr: Decimal
    source_rule_version: str
    source_protocol_version: str
    anchor_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_ANCHOR_SCHEMA_VERSION)
    market: Market = field(init=False, default=Market.FUTURES)
    venue: VenueV2 = field(init=False, default=VenueV2.USDM_FUTURES)
    interval: Literal["5m"] = field(init=False, default="5m")

    def __post_init__(self) -> None:
        _validate_anchor_material(self)
        object.__setattr__(
            self,
            "anchor_sha256",
            hashlib.sha256(
                _ANCHOR_DOMAIN + canonical_json_line(_anchor_document(self, False))
            ).hexdigest(),
        )

    @property
    def bar_open_ms(self) -> int:
        return self.decision_time_ms - FIVE_MINUTE_MS_V2 + 1

    @property
    def bar_close_ms(self) -> int:
        return self.decision_time_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self.decision_time_ms


@dataclass(frozen=True, slots=True)
class HistoricalExecutionContractV2:
    """Exact display-cost contract; it owns no fills or outcome observations."""

    fee_bps_per_side: int = _FROZEN_FEE_BPS_PER_SIDE
    slippage_bps_per_side: int = _FROZEN_SLIPPAGE_BPS_PER_SIDE
    execution_contract_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_EXECUTION_SCHEMA_VERSION)
    rule_version: str = field(init=False, default=_EXECUTION_RULE_VERSION)
    venue: VenueV2 = field(init=False, default=VenueV2.USDM_FUTURES)
    interval: Literal["5m"] = field(init=False, default="5m")
    public_market_data_only: Literal[True] = field(init=False, default=True)
    funding_in_decision_context: Literal[False] = field(init=False, default=False)
    directional_vote: Literal[False] = field(init=False, default=False)
    selection_gate_applied: Literal[False] = field(init=False, default=False)
    outcome_data_used: Literal[False] = field(init=False, default=False)
    order_placement: Literal[False] = field(init=False, default=False)

    def __post_init__(self) -> None:
        _validate_execution_material(self)
        object.__setattr__(
            self,
            "execution_contract_sha256",
            hashlib.sha256(
                _EXECUTION_DOMAIN
                + canonical_json_line(_execution_document(self, False))
            ).hexdigest(),
        )

    @property
    def zero_move_round_trip_cost_micros(self) -> int:
        return (
            _ROUND_TRIP_SIDES
            * (self.fee_bps_per_side + self.slippage_bps_per_side)
            * _BPS_TO_RETURN_MICROS
        )


@dataclass(frozen=True, slots=True)
class HistoricalDirectionalLeafV2:
    """One factory-sealed absolute-market directional proxy leaf."""

    family: HistoricalFamilyV2
    status: HistoricalLeafStatusV2
    direction: int | None
    strength_micros: int | None
    source_rule_version: str
    source_payload_sha256: str
    source_slice_sha256: str
    symbol: str
    venue: VenueV2
    interval: Literal["5m"]
    bar_open_ms: int
    bar_close_ms: int
    historical_slice_through_ms: int
    source_representation: HistoricalLeafSourceRepresentationV2
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    leaf_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_LEAF_SCHEMA_VERSION)
    absolute_market_direction: Literal[True] = field(init=False, default=True)
    primary_direction_used: Literal[False] = field(init=False, default=False)
    outcome_data_used: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _LEAF_FACTORY_TOKEN:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical directional leaves require their proxy factory"
            )
        _validate_leaf(self)
        object.__setattr__(
            self,
            "leaf_sha256",
            hashlib.sha256(
                _LEAF_DOMAIN + canonical_json_line(_leaf_document(self, False))
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is HistoricalLeafStatusV2.READY


@dataclass(frozen=True, slots=True)
class HistoricalCostSurvivalContextV2:
    """Display-only ATR headroom after the exact zero-move round-trip cost."""

    execution_contract_sha256: str
    anchor_sha256: str
    status: HistoricalCostSurvivalStatusV2
    zero_move_round_trip_cost_micros: int | None
    atr_fraction_micros: int | None
    one_atr_cost_headroom_micros: int | None
    _factory_token: InitVar[object | None] = None
    context_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_COST_SCHEMA_VERSION)
    funding_included: Literal[False] = field(init=False, default=False)
    directional_vote: Literal[False] = field(init=False, default=False)
    selection_gate_applied: Literal[False] = field(init=False, default=False)
    outcome_data_used: Literal[False] = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _COST_FACTORY_TOKEN:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical cost contexts require their frozen factory"
            )
        _validate_cost_context(self)
        object.__setattr__(
            self,
            "context_sha256",
            hashlib.sha256(
                _COST_DOMAIN + canonical_json_line(_cost_document(self, False))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class HistoricalDirectionalAggregationV2:
    """Pure three-value arithmetic used by both consensus and audit tests."""

    state_class: DirectionalStateClassV2
    directional_numerator_micros: int
    directional_denominator: Literal[3]
    directional_agreement_micros: int
    bullish_family_count: int
    bearish_family_count: int
    neutral_family_count: int
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _AGGREGATION_FACTORY_TOKEN:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical aggregation values require their frozen calculator"
            )


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyConsensusV2:
    """Hash-bound historical census decision with no efficacy authority."""

    anchor: HistoricalRecommendationAnchorV2 = field(repr=False)
    leaves: tuple[HistoricalDirectionalLeafV2, ...]
    execution_contract: HistoricalExecutionContractV2
    cost_context: HistoricalCostSurvivalContextV2
    experiment_contract_sha256: str
    cross_peer_symbols: tuple[str, ...]
    cross_peer_path_sha256s: tuple[tuple[str, str], ...]
    cross_peer_set_root_sha256: str
    cross_peer_input_sha256: str
    status: HistoricalConsensusStatusV2
    state_class: DirectionalStateClassV2
    directional_numerator_micros: int | None
    directional_denominator: int | None
    directional_agreement_micros: int | None
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    primary_relationship: PrimaryRelationshipV2
    admitted: bool
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    )
    role: str = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_CONSENSUS_ROLE_V2,
    )
    source_authority_status: str = field(
        init=False,
        default=_SOURCE_AUTHORITY_STATUS,
    )
    invalidation: str = field(init=False, default=_FIXED_INVALIDATION)
    historical_only: Literal[True] = field(init=False, default=True)
    historical_diagnostic_only: Literal[True] = field(init=False, default=True)
    shadow_only: Literal[True] = field(init=False, default=True)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    order_instruction: Literal[False] = field(init=False, default=False)
    order_placement: Literal[False] = field(init=False, default=False)
    paper_executable: Literal[False] = field(init=False, default=False)
    deployment_approved: Literal[False] = field(init=False, default=False)
    outcome_data_used: Literal[False] = field(init=False, default=False)
    changes_source_decision: Literal[False] = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CONSENSUS_FACTORY_TOKEN:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical consensus decisions require their frozen factory"
            )
        _validate_consensus(self, require_identity=False)
        object.__setattr__(self, "event_id", _consensus_event_id(self))
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _PAYLOAD_DOMAIN
                + canonical_json_line(_consensus_document(self, False))
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is HistoricalConsensusStatusV2.READY

    @property
    def bar_open_ms(self) -> int:
        return self.anchor.bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self.anchor.bar_close_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self.anchor.decision_cutoff_ms


type HistoricalProxyV2 = (
    PriceHistoricalKlineProxyV2
    | ParticipationHistoricalKlineProxyV2
    | HistoricalCrossSectional7AssetProxyV2
)


class HistoricalThreeFamilyConsensusRegistryV2:
    """Bounded idempotence/collision gate for historical census rows."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "maximum_events must be a positive integer"
            )
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    def register(
        self,
        value: HistoricalThreeFamilyConsensusV2,
    ) -> HistoricalConsensusRegistryDispositionV2:
        if type(value) is not HistoricalThreeFamilyConsensusV2:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "registry accepts exact HistoricalThreeFamilyConsensusV2 values only"
            )
        payload = canonical_historical_three_family_consensus_v2(value)
        prior = self._payload_by_event_id.get(value.event_id)
        if prior is not None:
            if prior != payload:
                raise HistoricalThreeFamilyConsensusContractErrorV2(
                    "deterministic historical consensus event ID collides with a different payload"
                )
            return HistoricalConsensusRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "bounded historical consensus registry capacity exhausted"
            )
        self._payload_by_event_id[value.event_id] = payload
        return HistoricalConsensusRegistryDispositionV2.NEW


def build_historical_execution_contract_v2() -> HistoricalExecutionContractV2:
    """Return the sole frozen 5+8 bp-per-side USD-M cost contract."""

    return HistoricalExecutionContractV2()


def build_historical_directional_leaf_v2(
    source_proxy: HistoricalProxyV2,
) -> HistoricalDirectionalLeafV2:
    """Validate one exact proxy and preserve its absolute market direction."""

    if type(source_proxy) is PriceHistoricalKlineProxyV2:
        _validate_price_proxy_summary(source_proxy)
        return build_historical_directional_leaf_from_calculation_v2(
            calculation=source_proxy.calculation,
            symbol=source_proxy.symbol,
            venue=VenueV2.USDM_FUTURES,
            interval=source_proxy.interval,
            bar_open_ms=source_proxy.bar_open_ms,
            bar_close_ms=source_proxy.bar_close_ms,
            historical_slice_through_ms=source_proxy.historical_slice_through_ms,
            source_slice_sha256=source_proxy.economic_close_slice_sha256,
        )
    if type(source_proxy) is ParticipationHistoricalKlineProxyV2:
        _validate_participation_proxy_summary(source_proxy)
        calculation = source_proxy.calculation
        if calculation is not None:
            return build_historical_directional_leaf_from_calculation_v2(
                calculation=calculation,
                symbol=source_proxy.symbol,
                venue=VenueV2.USDM_FUTURES,
                interval=source_proxy.interval,
                bar_open_ms=source_proxy.bar_open_ms,
                bar_close_ms=source_proxy.bar_close_ms,
                historical_slice_through_ms=(
                    source_proxy.historical_slice_through_ms
                ),
                source_slice_sha256=source_proxy.economic_flow_root_sha256,
            )
        status = _participation_leaf_status(source_proxy)
        return _leaf(
            family=HistoricalFamilyV2.PARTICIPATION_FLOW,
            status=status,
            direction=None,
            strength=None,
            source_rule_version=source_proxy.rule_version,
            source_payload_sha256=source_proxy.projection_sha256,
            source_slice_sha256=source_proxy.economic_flow_root_sha256,
            symbol=source_proxy.symbol,
            venue=VenueV2.USDM_FUTURES,
            interval=source_proxy.interval,
            bar_open_ms=source_proxy.bar_open_ms,
            bar_close_ms=source_proxy.bar_close_ms,
            historical_slice_through_ms=source_proxy.historical_slice_through_ms,
            source_representation=(
                HistoricalLeafSourceRepresentationV2.SEALED_PROXY_UNAVAILABLE
            ),
        )
    if type(source_proxy) is HistoricalCrossSectional7AssetProxyV2:
        _validate_cross_proxy_summary(source_proxy)
        status = _cross_leaf_status(source_proxy)
        return _leaf(
            family=HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET,
            status=status,
            direction=source_proxy.direction if status is HistoricalLeafStatusV2.READY else None,
            strength=(
                source_proxy.strength_micros
                if status is HistoricalLeafStatusV2.READY
                else None
            ),
            source_rule_version=source_proxy.rule_version,
            source_payload_sha256=source_proxy.calculation_sha256,
            source_slice_sha256=source_proxy.source_input.input_sha256,
            symbol=source_proxy.target_symbol,
            venue=VenueV2.USDM_FUTURES,
            interval="5m",
            bar_open_ms=source_proxy.source_input.final_decision_bar_open_ms,
            bar_close_ms=source_proxy.source_input.final_decision_bar_close_ms,
            historical_slice_through_ms=(
                source_proxy.source_input.final_decision_bar_close_ms
            ),
            source_representation=(
                HistoricalLeafSourceRepresentationV2.CANONICAL_NUMERIC_CALCULATION
            ),
        )
    raise HistoricalThreeFamilyConsensusContractErrorV2(
        "source_proxy must be one exact supported historical proxy type"
    )


def build_historical_directional_leaf_from_calculation_v2(
    *,
    calculation: (
        PriceClosePathCalculationV2
        | ParticipationFlowCalculationV2
        | HistoricalCrossSectional7AssetCalculationV2
    ),
    symbol: str,
    venue: VenueV2,
    interval: Literal["5m"],
    bar_open_ms: int,
    bar_close_ms: int,
    historical_slice_through_ms: int,
    source_slice_sha256: str,
) -> HistoricalDirectionalLeafV2:
    """Seal one canonical numeric calculation into an auditable scoped leaf."""

    _validate_calculation_scope(
        symbol=symbol,
        venue=venue,
        interval=interval,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        historical_slice_through_ms=historical_slice_through_ms,
        source_slice_sha256=source_slice_sha256,
    )
    if type(calculation) is PriceClosePathCalculationV2:
        try:
            canonical_price_close_path_calculation_v2(calculation)
        except PriceEvidenceContractErrorV2 as exc:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "price calculation failed canonical validation"
            ) from exc
        family = HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM
        status = _price_calculation_leaf_status(calculation)
    elif type(calculation) is ParticipationFlowCalculationV2:
        try:
            canonical_participation_flow_calculation_v2(calculation)
        except ParticipationFlowContractErrorV2 as exc:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "participation calculation failed canonical validation"
            ) from exc
        family = HistoricalFamilyV2.PARTICIPATION_FLOW
        status = _participation_calculation_leaf_status(calculation)
    elif type(calculation) is HistoricalCrossSectional7AssetCalculationV2:
        try:
            canonical_historical_cross_sectional_7asset_calculation_v2(calculation)
        except HistoricalCrossSectional7AssetProxyContractErrorV2 as exc:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "cross-sectional calculation failed canonical validation"
            ) from exc
        family = HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET
        status = _cross_calculation_leaf_status(calculation)
    else:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "calculation must be one exact supported factory-sealed type"
        )
    ready = status is HistoricalLeafStatusV2.READY
    return _leaf(
        family=family,
        status=status,
        direction=calculation.direction if ready else None,
        strength=calculation.strength_micros if ready else None,
        source_rule_version=calculation.rule_version,
        source_payload_sha256=calculation.calculation_sha256,
        source_slice_sha256=source_slice_sha256,
        symbol=symbol,
        venue=venue,
        interval=interval,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        historical_slice_through_ms=historical_slice_through_ms,
        source_representation=(
            HistoricalLeafSourceRepresentationV2.CANONICAL_NUMERIC_CALCULATION
        ),
    )


def calculate_historical_three_family_aggregation_v2(
    values: tuple[tuple[int, int], ...],
) -> HistoricalDirectionalAggregationV2:
    """Apply the frozen sign-count classification and ties-away arithmetic."""

    if type(values) is not tuple or len(values) != 3:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "aggregation requires exactly three immutable directional values"
        )
    for value in values:
        if type(value) is not tuple or len(value) != 2:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "each directional value must be an exact direction-strength pair"
            )
        direction, strength = value
        _validate_ready_direction_strength(direction, strength)
    numerator = sum(direction * strength for direction, strength in values)
    bullish = sum(direction == 1 for direction, _ in values)
    bearish = sum(direction == -1 for direction, _ in values)
    neutral = 3 - bullish - bearish
    return HistoricalDirectionalAggregationV2(
        state_class=_state_class(bullish_count=bullish, bearish_count=bearish),
        directional_numerator_micros=numerator,
        directional_denominator=3,
        directional_agreement_micros=_round_nearest_away_from_zero(numerator, 3),
        bullish_family_count=bullish,
        bearish_family_count=bearish,
        neutral_family_count=neutral,
        _factory_token=_AGGREGATION_FACTORY_TOKEN,
    )


def resolve_historical_consensus_status_v2(
    statuses: tuple[HistoricalLeafStatusV2, ...],
) -> HistoricalConsensusStatusV2:
    """Resolve exact three-leaf readiness with fail-closed severity precedence."""

    if type(statuses) is not tuple or len(statuses) != 3 or any(
        type(status) is not HistoricalLeafStatusV2 for status in statuses
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "status resolution requires exactly three HistoricalLeafStatusV2 values"
        )
    if HistoricalLeafStatusV2.DATA_INVALID in statuses:
        return HistoricalConsensusStatusV2.DATA_INVALID
    if HistoricalLeafStatusV2.INCONCLUSIVE_DATA in statuses:
        return HistoricalConsensusStatusV2.INCONCLUSIVE_DATA
    if HistoricalLeafStatusV2.FEATURE_NOT_READY in statuses:
        return HistoricalConsensusStatusV2.FEATURE_NOT_READY
    return HistoricalConsensusStatusV2.READY


def build_historical_cost_survival_context_v2(
    *,
    anchor: HistoricalRecommendationAnchorV2,
    execution_contract: HistoricalExecutionContractV2,
) -> HistoricalCostSurvivalContextV2:
    """Derive display-only one-ATR cost headroom without selecting on it."""

    _validate_anchor(anchor)
    _validate_execution(execution_contract)
    try:
        with localcontext(protocol_decimal_context_v2()):
            atr_fraction = anchor.atr / anchor.price
            atr_fraction_micros = int(
                (atr_fraction * Decimal(_STRENGTH_SCALE)).to_integral_value(
                    rounding=ROUND_HALF_UP
                )
            )
    except DecimalException as exc:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "ATR cost-survival context violates the Decimal34 contract"
        ) from exc
    zero_move = execution_contract.zero_move_round_trip_cost_micros
    return HistoricalCostSurvivalContextV2(
        execution_contract_sha256=execution_contract.execution_contract_sha256,
        anchor_sha256=anchor.anchor_sha256,
        status=HistoricalCostSurvivalStatusV2.AVAILABLE,
        zero_move_round_trip_cost_micros=zero_move,
        atr_fraction_micros=atr_fraction_micros,
        one_atr_cost_headroom_micros=atr_fraction_micros - zero_move,
        _factory_token=_COST_FACTORY_TOKEN,
    )


def build_historical_three_family_consensus_v2(
    *,
    anchor: HistoricalRecommendationAnchorV2,
    source_proxies: tuple[HistoricalProxyV2, ...],
    execution_contract: HistoricalExecutionContractV2,
    experiment_contract_sha256: str,
) -> HistoricalThreeFamilyConsensusV2:
    """Bind and evaluate exactly one proxy from each independent family."""

    _validate_anchor(anchor)
    _validate_execution(execution_contract)
    _validate_sha256(experiment_contract_sha256, "experiment_contract_sha256")
    ordered_proxies = _ordered_proxies(source_proxies)
    price_proxy, participation_proxy, cross_proxy = ordered_proxies
    assert type(price_proxy) is PriceHistoricalKlineProxyV2
    assert type(participation_proxy) is ParticipationHistoricalKlineProxyV2
    assert type(cross_proxy) is HistoricalCrossSectional7AssetProxyV2
    _validate_proxy_scope(anchor, price_proxy, participation_proxy, cross_proxy)
    leaves = tuple(build_historical_directional_leaf_v2(value) for value in ordered_proxies)
    peer_paths = tuple(
        (path.symbol, path.path_sha256) for path in cross_proxy.source_input.peer_paths
    )
    return build_historical_three_family_consensus_from_leaves_v2(
        anchor=anchor,
        leaves=leaves,
        execution_contract=execution_contract,
        experiment_contract_sha256=experiment_contract_sha256,
        cross_peer_path_sha256s=peer_paths,
        cross_peer_input_sha256=cross_proxy.source_input.input_sha256,
    )


def build_historical_three_family_consensus_from_leaves_v2(
    *,
    anchor: HistoricalRecommendationAnchorV2,
    leaves: tuple[HistoricalDirectionalLeafV2, ...],
    execution_contract: HistoricalExecutionContractV2,
    experiment_contract_sha256: str,
    cross_peer_path_sha256s: tuple[tuple[str, str], ...],
    cross_peer_input_sha256: str,
) -> HistoricalThreeFamilyConsensusV2:
    """Build consensus from three factory-sealed scoped calculation leaves."""

    _validate_anchor(anchor)
    _validate_execution(execution_contract)
    _validate_sha256(experiment_contract_sha256, "experiment_contract_sha256")
    ordered_leaves = _ordered_leaves(leaves)
    _validate_leaf_scopes(anchor, ordered_leaves)
    peer_paths = _canonical_peer_paths(
        anchor=anchor,
        values=cross_peer_path_sha256s,
    )
    _validate_sha256(cross_peer_input_sha256, "cross_peer_input_sha256")
    cross_leaf = ordered_leaves[2]
    if cross_leaf.source_slice_sha256 != cross_peer_input_sha256:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross leaf source slice must equal the target-excluded peer input hash"
        )
    cost_context = build_historical_cost_survival_context_v2(
        anchor=anchor,
        execution_contract=execution_contract,
    )
    derived = _derive_consensus(ordered_leaves, anchor.primary_direction)
    peer_root = _cross_peer_root(anchor.symbol, peer_paths)
    return HistoricalThreeFamilyConsensusV2(
        anchor=anchor,
        leaves=ordered_leaves,
        execution_contract=execution_contract,
        cost_context=cost_context,
        experiment_contract_sha256=experiment_contract_sha256,
        cross_peer_symbols=tuple(symbol for symbol, _ in peer_paths),
        cross_peer_path_sha256s=peer_paths,
        cross_peer_set_root_sha256=peer_root,
        cross_peer_input_sha256=cross_peer_input_sha256,
        status=derived[0],
        state_class=derived[1],
        directional_numerator_micros=derived[2],
        directional_denominator=derived[3],
        directional_agreement_micros=derived[4],
        bullish_family_count=derived[5],
        bearish_family_count=derived[6],
        neutral_family_count=derived[7],
        primary_relationship=derived[8],
        admitted=derived[9],
        reasons=derived[10],
        _factory_token=_CONSENSUS_FACTORY_TOKEN,
    )


def canonical_historical_three_family_consensus_v2(
    value: HistoricalThreeFamilyConsensusV2,
) -> bytes:
    """Revalidate and serialize one self-hashed historical consensus row."""

    if type(value) is not HistoricalThreeFamilyConsensusV2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "value must be an exact HistoricalThreeFamilyConsensusV2"
        )
    _validate_consensus(value, require_identity=True)
    payload = canonical_json_line(_consensus_document(value, False))
    expected = hashlib.sha256(_PAYLOAD_DOMAIN + payload).hexdigest()
    if value.payload_sha256 != expected:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical consensus payload hash differs from canonical content"
        )
    return canonical_json_line(_consensus_document(value, True))


def _leaf(
    *,
    family: HistoricalFamilyV2,
    status: HistoricalLeafStatusV2,
    direction: int | None,
    strength: int | None,
    source_rule_version: str,
    source_payload_sha256: str,
    source_slice_sha256: str,
    symbol: str,
    venue: VenueV2,
    interval: Literal["5m"],
    bar_open_ms: int,
    bar_close_ms: int,
    historical_slice_through_ms: int,
    source_representation: HistoricalLeafSourceRepresentationV2,
) -> HistoricalDirectionalLeafV2:
    return HistoricalDirectionalLeafV2(
        family=family,
        status=status,
        direction=direction,
        strength_micros=strength,
        source_rule_version=source_rule_version,
        source_payload_sha256=source_payload_sha256,
        source_slice_sha256=source_slice_sha256,
        symbol=symbol,
        venue=venue,
        interval=interval,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        historical_slice_through_ms=historical_slice_through_ms,
        source_representation=source_representation,
        reasons=(
            f"HISTORICAL_{family.value}_LEAF_{status.value}",
            f"SOURCE_REPRESENTATION_{source_representation.value}",
            "EXACT_CLOSED_5M_SCOPE_AND_SOURCE_SLICE_HASH_BOUND",
            "ABSOLUTE_MARKET_DIRECTION_PRIMARY_SIDE_NOT_USED",
            "HISTORICAL_NONPROMOTING_NO_PROBABILITY_OR_ORDER_CLAIM",
        ),
        _factory_token=_LEAF_FACTORY_TOKEN,
    )


def _validate_price_proxy_summary(value: PriceHistoricalKlineProxyV2) -> None:
    """Check the exact factory-sealed price output without replaying 8,653 rows."""

    if (
        value.rule_version != PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2
        or type(value.status) is not PriceHistoricalKlineProxyStatusV2
        or value.market is not Market.FUTURES
        or value.interval != "5m"
        or value.historical_slice_through_ms != value.bar_close_ms
        or value.calculation.rule_version != PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2
        or value.direction != value.calculation.direction
        or value.strength_micros != value.calculation.strength_micros
        or value.calculation_ready
        != (value.status is PriceHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY)
        or value.historical_only is not True
        or value.historical_diagnostic_only is not True
        or value.outcome_data_read is not False
        or value.target_return_used is not False
        or value.producer_ready is not False
        or value.promoting_eligible is not False
        or value.probability_eligible is not False
        or value.probability_calibrated is not False
        or value.data_through_ms is not None
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "price historical proxy sealed summary is inconsistent"
        )
    _validate_ready_direction_strength(value.direction, value.strength_micros)
    _validate_sha256(value.proxy_sha256, "price proxy_sha256")
    _validate_sha256(
        value.source_lineage_root_sha256,
        "price source_lineage_root_sha256",
    )
    _validate_sha256(
        value.economic_close_slice_sha256,
        "price economic_close_slice_sha256",
    )
    _validate_sha256(
        value.calculation.calculation_sha256,
        "price calculation_sha256",
    )


def _validate_participation_proxy_summary(
    value: ParticipationHistoricalKlineProxyV2,
) -> None:
    """Check the exact factory-sealed flow output without replaying 8,641 rows."""

    if (
        value.rule_version != PARTICIPATION_FLOW_RULE_VERSION_V2
        or type(value.status) is not ParticipationHistoricalKlineProxyStatusV2
        or value.market is not Market.FUTURES
        or value.interval != "5m"
        or value.historical_slice_through_ms != value.bar_close_ms
        or value.historical_diagnostic_only is not True
        or value.outcome_data_read is not False
        or value.producer_ready is not False
        or value.promoting_eligible is not False
        or value.probability_eligible is not False
        or value.data_through_ms is not None
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "participation historical proxy sealed summary is inconsistent"
        )
    _validate_sha256(value.projection_sha256, "participation projection_sha256")
    _validate_sha256(
        value.source_lineage_root_sha256,
        "participation source_lineage_root_sha256",
    )
    _validate_sha256(
        value.economic_flow_root_sha256,
        "participation economic_flow_root_sha256",
    )
    calculation = value.calculation
    if calculation is None:
        if value.calculation_ready:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "participation proxy cannot be ready without a calculation"
            )
        return
    if calculation.rule_version != PARTICIPATION_FLOW_RULE_VERSION_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "participation calculation rule version differs"
        )
    _validate_ready_direction_strength(
        calculation.direction,
        calculation.strength_micros,
    )
    _validate_sha256(
        calculation.calculation_sha256,
        "participation calculation_sha256",
    )
    if value.calculation_ready != (
        value.status is ParticipationHistoricalKlineProxyStatusV2.NUMERIC_READY_PROXY
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "participation proxy status and calculation readiness differ"
        )


def _validate_cross_proxy_summary(
    value: HistoricalCrossSectional7AssetProxyV2,
) -> None:
    """Check the exact factory-sealed cross output without replaying peer paths."""

    if (
        value.rule_version != HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2
        or type(value.status) is not HistoricalCrossSectional7AssetProxyStatusV2
        or value.historical_only is not True
        or value.shadow_only is not True
        or value.live_authority is not False
        or value.producer_ready is not False
        or value.paper_executable is not False
        or value.promoting is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.target_candles_used is not False
        or value.target_return_used is not False
        or value.primary_direction_used is not False
        or value.outcome_used is not False
        or value.data_through_ms is not None
        or value.signed_strength_micros
        != value.direction * value.strength_micros
        or value.ready
        != (value.status is HistoricalCrossSectional7AssetProxyStatusV2.READY)
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross-sectional historical proxy sealed summary is inconsistent"
        )
    _validate_ready_direction_strength(value.direction, value.strength_micros)
    _validate_sha256(value.event_id, "cross proxy event_id")
    _validate_sha256(value.proxy_sha256, "cross proxy_sha256")
    _validate_sha256(value.calculation_sha256, "cross calculation_sha256")
    _validate_sha256(value.source_input.input_sha256, "cross input_sha256")
    for path in value.source_input.peer_paths:
        _validate_sha256(path.path_sha256, "cross peer path_sha256")


def _validate_calculation_scope(
    *,
    symbol: str,
    venue: VenueV2,
    interval: str,
    bar_open_ms: int,
    bar_close_ms: int,
    historical_slice_through_ms: int,
    source_slice_sha256: str,
) -> None:
    _validate_symbol(symbol, "calculation symbol")
    if venue is not VenueV2.USDM_FUTURES:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical calculation scope requires USD-M Futures"
        )
    if interval != "5m":
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical calculation scope requires interval 5m"
        )
    _validate_nonnegative_int(bar_open_ms, "calculation bar_open_ms")
    _validate_nonnegative_int(bar_close_ms, "calculation bar_close_ms")
    _validate_nonnegative_int(
        historical_slice_through_ms,
        "calculation historical_slice_through_ms",
    )
    if (
        bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or bar_close_ms != bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        or historical_slice_through_ms != bar_close_ms
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical calculation scope must end at one fully closed 5m bar"
        )
    _validate_sha256(source_slice_sha256, "source_slice_sha256")


def _price_calculation_leaf_status(
    value: PriceClosePathCalculationV2,
) -> HistoricalLeafStatusV2:
    if value.status is RobustZStatusV2.READY:
        return HistoricalLeafStatusV2.READY
    if value.status is RobustZStatusV2.DATA_INVALID_FEATURE:
        return HistoricalLeafStatusV2.DATA_INVALID
    return HistoricalLeafStatusV2.FEATURE_NOT_READY


def _participation_calculation_leaf_status(
    value: ParticipationFlowCalculationV2,
) -> HistoricalLeafStatusV2:
    if value.status is ParticipationFlowStatusV2.READY:
        return HistoricalLeafStatusV2.READY
    if value.status is ParticipationFlowStatusV2.INCONCLUSIVE_DATA:
        return HistoricalLeafStatusV2.INCONCLUSIVE_DATA
    if value.status is ParticipationFlowStatusV2.DATA_INVALID_ARITHMETIC:
        return HistoricalLeafStatusV2.DATA_INVALID
    return HistoricalLeafStatusV2.FEATURE_NOT_READY


def _cross_calculation_leaf_status(
    value: HistoricalCrossSectional7AssetCalculationV2,
) -> HistoricalLeafStatusV2:
    if value.status is HistoricalCrossSectional7AssetProxyStatusV2.READY:
        return HistoricalLeafStatusV2.READY
    if (
        value.status
        is HistoricalCrossSectional7AssetProxyStatusV2.DATA_INVALID_ARITHMETIC
    ):
        return HistoricalLeafStatusV2.DATA_INVALID
    return HistoricalLeafStatusV2.FEATURE_NOT_READY


def _price_leaf_status(value: PriceHistoricalKlineProxyV2) -> HistoricalLeafStatusV2:
    return _price_calculation_leaf_status(value.calculation)


def _participation_leaf_status(
    value: ParticipationHistoricalKlineProxyV2,
) -> HistoricalLeafStatusV2:
    if value.status in {
        ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_MISSING_SLOT_UNKNOWN,
        ParticipationHistoricalKlineProxyStatusV2.UNAVAILABLE_INTERNAL_GAP_UNKNOWN,
    }:
        return HistoricalLeafStatusV2.INCONCLUSIVE_DATA
    calculation = value.calculation
    if calculation is None:
        return HistoricalLeafStatusV2.INCONCLUSIVE_DATA
    return _participation_calculation_leaf_status(calculation)


def _cross_leaf_status(
    value: HistoricalCrossSectional7AssetProxyV2,
) -> HistoricalLeafStatusV2:
    if value.status is HistoricalCrossSectional7AssetProxyStatusV2.READY:
        return HistoricalLeafStatusV2.READY
    if (
        value.status
        is HistoricalCrossSectional7AssetProxyStatusV2.DATA_INVALID_ARITHMETIC
    ):
        return HistoricalLeafStatusV2.DATA_INVALID
    return HistoricalLeafStatusV2.FEATURE_NOT_READY


def _ordered_proxies(
    values: tuple[HistoricalProxyV2, ...],
) -> tuple[
    PriceHistoricalKlineProxyV2,
    ParticipationHistoricalKlineProxyV2,
    HistoricalCrossSectional7AssetProxyV2,
]:
    if type(values) is not tuple or len(values) != 3:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "source_proxies must be an immutable tuple of exactly three values"
        )
    by_type: dict[type[object], HistoricalProxyV2] = {}
    supported = {
        PriceHistoricalKlineProxyV2,
        ParticipationHistoricalKlineProxyV2,
        HistoricalCrossSectional7AssetProxyV2,
    }
    for value in values:
        if type(value) not in supported:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "source_proxies contains an unsupported or subclassed value"
            )
        if type(value) in by_type:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "source_proxies must contain exactly one proxy per family"
            )
        by_type[type(value)] = value
    if set(by_type) != supported:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "source_proxies must contain price, participation, and cross-section"
        )
    return (
        cast(PriceHistoricalKlineProxyV2, by_type[PriceHistoricalKlineProxyV2]),
        cast(
            ParticipationHistoricalKlineProxyV2,
            by_type[ParticipationHistoricalKlineProxyV2],
        ),
        cast(
            HistoricalCrossSectional7AssetProxyV2,
            by_type[HistoricalCrossSectional7AssetProxyV2],
        ),
    )


def _ordered_leaves(
    values: tuple[HistoricalDirectionalLeafV2, ...],
) -> tuple[HistoricalDirectionalLeafV2, ...]:
    if type(values) is not tuple or len(values) != 3:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "leaves must be an immutable tuple of exactly three values"
        )
    by_family: dict[HistoricalFamilyV2, HistoricalDirectionalLeafV2] = {}
    for value in values:
        if type(value) is not HistoricalDirectionalLeafV2:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "leaves contains an unsupported or subclassed value"
            )
        _validate_leaf(value)
        expected_hash = hashlib.sha256(
            _LEAF_DOMAIN + canonical_json_line(_leaf_document(value, False))
        ).hexdigest()
        if value.leaf_sha256 != expected_hash:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical directional leaf hash differs"
            )
        if value.family in by_family:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "leaves must contain exactly one leaf per family"
            )
        by_family[value.family] = value
    expected_families = tuple(HistoricalFamilyV2(item) for item in _FAMILY_ORDER)
    if set(by_family) != set(expected_families):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "leaves must contain price, participation, and cross-section"
        )
    return tuple(by_family[family] for family in expected_families)


def _validate_leaf_scopes(
    anchor: HistoricalRecommendationAnchorV2,
    leaves: tuple[HistoricalDirectionalLeafV2, ...],
) -> None:
    for leaf in leaves:
        if (
            leaf.symbol != anchor.symbol
            or leaf.venue is not VenueV2.USDM_FUTURES
            or leaf.interval != anchor.interval
            or leaf.bar_open_ms != anchor.bar_open_ms
            or leaf.bar_close_ms != anchor.bar_close_ms
            or leaf.historical_slice_through_ms != anchor.bar_close_ms
        ):
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                f"{leaf.family.value} leaf scope differs from the anchor"
            )


def _canonical_peer_paths(
    *,
    anchor: HistoricalRecommendationAnchorV2,
    values: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    if type(values) is not tuple or len(values) != 6:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross peer paths must contain exactly six immutable entries"
        )
    for value in values:
        if type(value) is not tuple or len(value) != 2:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "cross peer path entries must be exact symbol-hash pairs"
            )
        _validate_symbol(value[0], "cross peer symbol")
        _validate_sha256(value[1], "cross peer path_sha256")
    ordered = tuple(sorted(values, key=lambda item: item[0].encode("utf-8")))
    expected_symbols = tuple(
        sorted(
            (
                symbol
                for symbol in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2
                if symbol != anchor.symbol
            ),
            key=lambda item: item.encode("utf-8"),
        )
    )
    if tuple(symbol for symbol, _ in ordered) != expected_symbols:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross peer paths are not the exact remaining frozen six"
        )
    return ordered


def _validate_proxy_scope(
    anchor: HistoricalRecommendationAnchorV2,
    price: PriceHistoricalKlineProxyV2,
    participation: ParticipationHistoricalKlineProxyV2,
    cross: HistoricalCrossSectional7AssetProxyV2,
) -> None:
    for label, value in (("price", price), ("participation", participation)):
        if value.market is not Market.FUTURES or value.interval != "5m":
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                f"{label} proxy must be a Futures 5m proxy"
            )
        if value.symbol != anchor.symbol:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                f"{label} proxy symbol differs from the anchor"
            )
        if (
            value.bar_open_ms != anchor.bar_open_ms
            or value.bar_close_ms != anchor.bar_close_ms
            or value.historical_slice_through_ms != anchor.bar_close_ms
        ):
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                f"{label} proxy decision-bar scope differs from the anchor"
            )
    if price.rule_version != PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "price proxy rule version differs from the frozen calculation"
        )
    if participation.rule_version != PARTICIPATION_FLOW_RULE_VERSION_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "participation proxy rule version differs from the frozen calculation"
        )
    source = cross.source_input
    if cross.rule_version != HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross-sectional proxy rule version differs"
        )
    if cross.target_symbol != anchor.symbol:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross-sectional target differs from the anchor"
        )
    if (
        source.final_decision_bar_open_ms != anchor.bar_open_ms
        or source.final_decision_bar_close_ms != anchor.bar_close_ms
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross-sectional decision-bar scope differs from the anchor"
        )
    expected_peers = tuple(
        sorted(
            (
                symbol
                for symbol in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2
                if symbol != anchor.symbol
            ),
            key=lambda value: value.encode("utf-8"),
        )
    )
    if cross.peer_symbols != expected_peers:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross-sectional peers are not the exact remaining frozen six"
        )


def _derive_consensus(
    leaves: tuple[HistoricalDirectionalLeafV2, ...],
    primary_direction: Direction,
) -> tuple[
    HistoricalConsensusStatusV2,
    DirectionalStateClassV2,
    int | None,
    int | None,
    int | None,
    int | None,
    int | None,
    int | None,
    PrimaryRelationshipV2,
    bool,
    tuple[str, ...],
]:
    missing_status = _missing_consensus_status(leaves)
    if missing_status is not None:
        return (
            missing_status,
            DirectionalStateClassV2.WITHHELD,
            None,
            None,
            None,
            None,
            None,
            None,
            PrimaryRelationshipV2.WITHHELD,
            False,
            (
                "HISTORICAL_THREE_FAMILY_CONSENSUS_WITHHELD",
                *(
                    f"{leaf.family.value}_{leaf.status.value}"
                    for leaf in leaves
                    if leaf.status is not HistoricalLeafStatusV2.READY
                ),
                "NONREADY_REQUIRED_FAMILY_IS_UNKNOWN_NOT_NEUTRAL",
                "HISTORICAL_NONPROMOTING_NO_PROBABILITY_OR_ORDER_CLAIM",
            ),
        )
    values: list[tuple[int, int]] = []
    for leaf in leaves:
        assert leaf.direction is not None
        assert leaf.strength_micros is not None
        values.append((leaf.direction, leaf.strength_micros))
    aggregate = calculate_historical_three_family_aggregation_v2(tuple(values))
    relationship = _primary_relationship(aggregate.state_class, primary_direction)
    admitted = relationship is PrimaryRelationshipV2.SUPPORTS_PRIMARY
    return (
        HistoricalConsensusStatusV2.READY,
        aggregate.state_class,
        aggregate.directional_numerator_micros,
        aggregate.directional_denominator,
        aggregate.directional_agreement_micros,
        aggregate.bullish_family_count,
        aggregate.bearish_family_count,
        aggregate.neutral_family_count,
        relationship,
        admitted,
        (
            "EXACT_THREE_INDEPENDENT_DIRECTIONAL_FAMILIES_AGGREGATED",
            "ABSOLUTE_MARKET_DIRECTIONS_CLASSIFIED_BEFORE_PRIMARY_RELATIONSHIP",
            f"STATE_CLASS_{aggregate.state_class.value}",
            f"PRIMARY_RELATIONSHIP_{relationship.value}",
            (
                "PRIMARY_MATCHED_AUDIT_ADMITTED"
                if admitted
                else "ALL_ANCHOR_CENSUS_RETAINED_NOT_ADMITTED"
            ),
            "COST_SURVIVAL_CONTEXT_DISPLAY_ONLY_NOT_A_DIRECTIONAL_VOTE_OR_GATE",
            "DIRECTIONAL_AGREEMENT_IS_NOT_A_PROBABILITY",
            "HISTORICAL_NONPROMOTING_NO_ORDER_CLAIM",
        ),
    )


def _missing_consensus_status(
    leaves: tuple[HistoricalDirectionalLeafV2, ...],
) -> HistoricalConsensusStatusV2 | None:
    resolved = resolve_historical_consensus_status_v2(
        tuple(leaf.status for leaf in leaves)
    )
    return None if resolved is HistoricalConsensusStatusV2.READY else resolved


def _state_class(
    *,
    bullish_count: int,
    bearish_count: int,
) -> DirectionalStateClassV2:
    if bullish_count == 3 and bearish_count == 0:
        return DirectionalStateClassV2.BROAD_BULLISH_STATE
    if bullish_count >= 2 and bearish_count == 0:
        return DirectionalStateClassV2.BULLISH_STATE_TILT
    if bearish_count == 3 and bullish_count == 0:
        return DirectionalStateClassV2.BROAD_BEARISH_STATE
    if bearish_count >= 2 and bullish_count == 0:
        return DirectionalStateClassV2.BEARISH_STATE_TILT
    return DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE


def _primary_relationship(
    state: DirectionalStateClassV2,
    primary_direction: Direction,
) -> PrimaryRelationshipV2:
    bullish = state in {
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        DirectionalStateClassV2.BULLISH_STATE_TILT,
    }
    bearish = state in {
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        DirectionalStateClassV2.BEARISH_STATE_TILT,
    }
    if not bullish and not bearish:
        return PrimaryRelationshipV2.MIXED_OR_NEUTRAL
    supports = (bullish and primary_direction is Direction.LONG) or (
        bearish and primary_direction is Direction.SHORT
    )
    if supports:
        return PrimaryRelationshipV2.SUPPORTS_PRIMARY
    return PrimaryRelationshipV2.OPPOSES_PRIMARY


def _round_nearest_away_from_zero(numerator: int, denominator: int) -> int:
    if type(denominator) is not int or denominator <= 0:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "directional denominator must be a positive integer"
        )
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _cross_peer_root(
    target_symbol: str,
    values: tuple[tuple[str, str], ...],
) -> str:
    return hashlib.sha256(
        _PEER_ROOT_DOMAIN
        + canonical_json_line(
            {
                "peer_paths": [
                    {"path_sha256": path_sha256, "symbol": symbol}
                    for symbol, path_sha256 in values
                ],
                "schema_version": "r4b_historical_consensus_peer_root_v2",
                "target_excluded": True,
                "target_symbol": target_symbol,
            }
        )
    ).hexdigest()


def _consensus_event_id(value: HistoricalThreeFamilyConsensusV2) -> str:
    anchor = value.anchor
    identity = {
        "asset": anchor.asset,
        "bar_close_ms": anchor.bar_close_ms,
        "bar_open_ms": anchor.bar_open_ms,
        "decision_cutoff_ms": anchor.decision_cutoff_ms,
        "experiment_contract_sha256": value.experiment_contract_sha256,
        "interval": anchor.interval,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "source_event_id": anchor.source_event_id,
        "source_protocol_version": anchor.source_protocol_version,
        "source_rule_version": anchor.source_rule_version,
        "split": anchor.split,
        "symbol": anchor.symbol,
        "venue": anchor.venue.value,
    }
    return hashlib.sha256(
        _EVENT_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()


def _validate_consensus(
    value: HistoricalThreeFamilyConsensusV2,
    *,
    require_identity: bool,
) -> None:
    _validate_anchor(value.anchor)
    _validate_execution(value.execution_contract)
    _validate_cost_context(value.cost_context)
    _validate_sha256(value.experiment_contract_sha256, "experiment_contract_sha256")
    if (
        value.schema_version != _SCHEMA_VERSION
        or value.rule_version != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2
        or value.role != HISTORICAL_THREE_FAMILY_CONSENSUS_ROLE_V2
        or value.source_authority_status != _SOURCE_AUTHORITY_STATUS
        or value.invalidation != _FIXED_INVALIDATION
        or value.historical_only is not True
        or value.historical_diagnostic_only is not True
        or value.shadow_only is not True
        or value.promoting is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.order_instruction is not False
        or value.order_placement is not False
        or value.paper_executable is not False
        or value.deployment_approved is not False
        or value.outcome_data_used is not False
        or value.changes_source_decision is not False
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical consensus authority or fixed false claims differ"
        )
    if type(value.leaves) is not tuple or tuple(
        leaf.family.value for leaf in value.leaves
    ) != _FAMILY_ORDER:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical leaves must use the exact three-family order"
        )
    for leaf in value.leaves:
        _validate_leaf(leaf)
        expected_hash = hashlib.sha256(
            _LEAF_DOMAIN + canonical_json_line(_leaf_document(leaf, False))
        ).hexdigest()
        if leaf.leaf_sha256 != expected_hash:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical directional leaf hash differs"
            )
        if leaf.historical_slice_through_ms != value.anchor.bar_close_ms:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "historical leaf slice does not end at the anchor close"
            )
    _validate_leaf_scopes(value.anchor, value.leaves)
    expected_cost = build_historical_cost_survival_context_v2(
        anchor=value.anchor,
        execution_contract=value.execution_contract,
    )
    if value.cost_context != expected_cost:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cost-survival context differs from the bound anchor and execution contract"
        )
    _validate_cross_peer_material(value)
    if value.leaves[2].source_slice_sha256 != value.cross_peer_input_sha256:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cross leaf source slice differs from peer input hash"
        )
    expected = _derive_consensus(value.leaves, value.anchor.primary_direction)
    observed = (
        value.status,
        value.state_class,
        value.directional_numerator_micros,
        value.directional_denominator,
        value.directional_agreement_micros,
        value.bullish_family_count,
        value.bearish_family_count,
        value.neutral_family_count,
        value.primary_relationship,
        value.admitted,
        value.reasons,
    )
    if observed != expected:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical consensus fields contradict the bound leaves and anchor"
        )
    if type(value.admitted) is not bool:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "admitted must be an exact boolean"
        )
    _validate_reasons(value.reasons)
    if require_identity and value.event_id != _consensus_event_id(value):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical consensus event ID differs"
        )


def _validate_cross_peer_material(value: HistoricalThreeFamilyConsensusV2) -> None:
    expected_peers = tuple(
        sorted(
            (
                symbol
                for symbol in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2
                if symbol != value.anchor.symbol
            ),
            key=lambda item: item.encode("utf-8"),
        )
    )
    if type(value.cross_peer_symbols) is not tuple or value.cross_peer_symbols != expected_peers:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "consensus peer symbols differ from the exact target-excluded panel"
        )
    if type(value.cross_peer_path_sha256s) is not tuple or tuple(
        symbol for symbol, _ in value.cross_peer_path_sha256s
    ) != expected_peers:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "consensus peer paths differ from canonical peer order"
        )
    for symbol, path_sha256 in value.cross_peer_path_sha256s:
        _validate_symbol(symbol, "cross peer symbol")
        _validate_sha256(path_sha256, "cross peer path_sha256")
    _validate_sha256(value.cross_peer_input_sha256, "cross_peer_input_sha256")
    expected_root = _cross_peer_root(
        value.anchor.symbol,
        value.cross_peer_path_sha256s,
    )
    if value.cross_peer_set_root_sha256 != expected_root:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "consensus target-excluded peer root differs"
        )


def _validate_anchor(value: object) -> None:
    if type(value) is not HistoricalRecommendationAnchorV2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor must be an exact HistoricalRecommendationAnchorV2"
        )
    _validate_anchor_material(value)
    expected = hashlib.sha256(
        _ANCHOR_DOMAIN + canonical_json_line(_anchor_document(value, False))
    ).hexdigest()
    if value.anchor_sha256 != expected:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical recommendation anchor hash differs"
        )


def _validate_anchor_material(value: HistoricalRecommendationAnchorV2) -> None:
    if value.schema_version != _ANCHOR_SCHEMA_VERSION:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor schema version differs"
        )
    if not _SOURCE_EVENT_ID_RE.fullmatch(value.source_event_id):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "source_event_id must be exactly 24 lowercase hexadecimal characters"
        )
    _validate_sha256(value.source_row_sha256, "source_row_sha256")
    _validate_sha256(
        value.source_replay_manifest_sha256,
        "source_replay_manifest_sha256",
    )
    if value.split not in _ALLOWED_SPLITS:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor split is outside the frozen historical intervals"
        )
    if not _ASSET_RE.fullmatch(value.asset):
        raise HistoricalThreeFamilyConsensusContractErrorV2("anchor asset is invalid")
    if value.cohort != "volatile":
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor cohort must be volatile"
        )
    _validate_symbol(value.symbol, "anchor symbol")
    if value.symbol not in HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor symbol is outside the frozen seven-asset panel"
        )
    if _ASSET_BY_SYMBOL[value.symbol] != value.asset:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor asset and Futures symbol differ"
        )
    if value.market is not Market.FUTURES or value.venue is not VenueV2.USDM_FUTURES:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor must be USD-M Futures"
        )
    if value.interval != "5m":
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor interval must be exactly 5m"
        )
    if type(value.primary_family) is not SignalFamily or value.primary_family not in {
        SignalFamily.PULLBACK_LONG,
        SignalFamily.PULLBACK_SHORT,
    }:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor family is outside the frozen V1A pullback population"
        )
    if type(value.primary_direction) is not Direction or value.primary_direction not in {
        Direction.LONG,
        Direction.SHORT,
    }:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor direction must be long or short"
        )
    expected_direction = (
        Direction.LONG
        if value.primary_family is SignalFamily.PULLBACK_LONG
        else Direction.SHORT
    )
    if value.primary_direction is not expected_direction:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor family and direction are incompatible"
        )
    _validate_nonnegative_int(value.decision_time_ms, "decision_time_ms")
    if value.decision_time_ms % FIVE_MINUTE_MS_V2 != FIVE_MINUTE_MS_V2 - 1:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor decision_time_ms must be a fully closed 5m candle time"
        )
    if value.bar_open_ms < 0:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor decision bar would precede Unix epoch"
        )
    if not _is_positive_finite_decimal(value.price):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor price must be a positive finite Decimal"
        )
    if not _is_nonnegative_finite_decimal(value.atr):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor ATR must be a nonnegative finite Decimal"
        )
    if value.invalidation is not None:
        if not _is_positive_finite_decimal(value.invalidation):
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "anchor invalidation must be a positive finite Decimal or None"
            )
    if value.source_rule_version != HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor source rule version differs from frozen V1A"
        )
    if (
        value.source_protocol_version
        != HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "anchor source protocol version differs from frozen V1A"
        )


def _validate_execution(value: object) -> None:
    if type(value) is not HistoricalExecutionContractV2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "execution_contract must be an exact HistoricalExecutionContractV2"
        )
    _validate_execution_material(value)
    expected = hashlib.sha256(
        _EXECUTION_DOMAIN + canonical_json_line(_execution_document(value, False))
    ).hexdigest()
    if value.execution_contract_sha256 != expected:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical execution contract hash differs"
        )


def _validate_execution_material(value: HistoricalExecutionContractV2) -> None:
    if (
        value.schema_version != _EXECUTION_SCHEMA_VERSION
        or value.rule_version != _EXECUTION_RULE_VERSION
        or value.venue is not VenueV2.USDM_FUTURES
        or value.interval != "5m"
        or type(value.fee_bps_per_side) is not int
        or value.fee_bps_per_side != _FROZEN_FEE_BPS_PER_SIDE
        or type(value.slippage_bps_per_side) is not int
        or value.slippage_bps_per_side != _FROZEN_SLIPPAGE_BPS_PER_SIDE
        or value.public_market_data_only is not True
        or value.funding_in_decision_context is not False
        or value.directional_vote is not False
        or value.selection_gate_applied is not False
        or value.outcome_data_used is not False
        or value.order_placement is not False
        or value.zero_move_round_trip_cost_micros
        != _FROZEN_ZERO_MOVE_COST_MICROS
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical execution contract differs from frozen 5+8 bp costs"
        )


def _validate_leaf(value: HistoricalDirectionalLeafV2) -> None:
    if type(value) is not HistoricalDirectionalLeafV2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "leaf must be an exact HistoricalDirectionalLeafV2"
        )
    if (
        value.schema_version != _LEAF_SCHEMA_VERSION
        or type(value.family) is not HistoricalFamilyV2
        or type(value.status) is not HistoricalLeafStatusV2
        or type(value.source_representation)
        is not HistoricalLeafSourceRepresentationV2
        or value.absolute_market_direction is not True
        or value.primary_direction_used is not False
        or value.outcome_data_used is not False
        or value.promoting is not False
        or value.probability is not False
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical leaf identity or fixed false claims differ"
        )
    expected_rule = {
        HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM: (
            PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2
        ),
        HistoricalFamilyV2.PARTICIPATION_FLOW: PARTICIPATION_FLOW_RULE_VERSION_V2,
        HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET: (
            HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2
        ),
    }[value.family]
    if value.source_rule_version != expected_rule:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "historical leaf source rule version differs"
        )
    _validate_sha256(value.source_payload_sha256, "source_payload_sha256")
    _validate_sha256(value.source_slice_sha256, "source_slice_sha256")
    _validate_calculation_scope(
        symbol=value.symbol,
        venue=value.venue,
        interval=value.interval,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        historical_slice_through_ms=value.historical_slice_through_ms,
        source_slice_sha256=value.source_slice_sha256,
    )
    if (
        value.source_representation
        is HistoricalLeafSourceRepresentationV2.SEALED_PROXY_UNAVAILABLE
        and value.status is HistoricalLeafStatusV2.READY
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "READY leaf requires a canonical numeric calculation"
        )
    _validate_nonnegative_int(
        value.historical_slice_through_ms,
        "historical_slice_through_ms",
    )
    _validate_reasons(value.reasons)
    if value.status is HistoricalLeafStatusV2.READY:
        _validate_ready_direction_strength(value.direction, value.strength_micros)
    elif value.direction is not None or value.strength_micros is not None:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "non-ready historical leaves must expose unknown direction and strength"
        )


def _validate_ready_direction_strength(direction: object, strength: object) -> None:
    if type(direction) is not int or direction not in (-1, 0, 1):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "READY direction must be exactly -1, 0, or 1"
        )
    if type(strength) is not int or not 0 <= strength <= _STRENGTH_SCALE:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "READY strength_micros must be an integer in [0, 1000000]"
        )
    if direction == 0 and strength != 0:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "neutral READY direction cannot carry directional magnitude"
        )


def _validate_cost_context(value: HistoricalCostSurvivalContextV2) -> None:
    if type(value) is not HistoricalCostSurvivalContextV2:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cost_context must be an exact HistoricalCostSurvivalContextV2"
        )
    if (
        value.schema_version != _COST_SCHEMA_VERSION
        or type(value.status) is not HistoricalCostSurvivalStatusV2
        or value.funding_included is not False
        or value.directional_vote is not False
        or value.selection_gate_applied is not False
        or value.outcome_data_used is not False
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cost-survival context status or context-only flags differ"
        )
    _validate_sha256(value.execution_contract_sha256, "execution_contract_sha256")
    _validate_sha256(value.anchor_sha256, "anchor_sha256")
    numeric = (
        value.zero_move_round_trip_cost_micros,
        value.atr_fraction_micros,
        value.one_atr_cost_headroom_micros,
    )
    if value.status is HistoricalCostSurvivalStatusV2.AVAILABLE:
        if any(type(item) is not int for item in numeric):
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "AVAILABLE cost context requires exact integer metrics"
            )
        if value.zero_move_round_trip_cost_micros != _FROZEN_ZERO_MOVE_COST_MICROS:
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "cost context zero-move round trip must be 2600 micros"
            )
        assert value.atr_fraction_micros is not None
        assert value.one_atr_cost_headroom_micros is not None
        if value.atr_fraction_micros < 0 or (
            value.one_atr_cost_headroom_micros
            != value.atr_fraction_micros - _FROZEN_ZERO_MOVE_COST_MICROS
        ):
            raise HistoricalThreeFamilyConsensusContractErrorV2(
                "cost context ATR headroom arithmetic differs"
            )
    elif any(item is not None for item in numeric):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "NOT_READY cost context cannot expose partial numeric metrics"
        )
    expected = hashlib.sha256(
        _COST_DOMAIN + canonical_json_line(_cost_document(value, False))
    ).hexdigest()
    if getattr(value, "context_sha256", expected) != expected:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "cost-survival context hash differs"
        )


def _anchor_document(
    value: HistoricalRecommendationAnchorV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "asset": value.asset,
        "atr": str(value.atr),
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "cohort": value.cohort,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "decision_time_ms": value.decision_time_ms,
        "interval": value.interval,
        "invalidation": None if value.invalidation is None else str(value.invalidation),
        "market": value.market.value,
        "price": str(value.price),
        "primary_direction": value.primary_direction.value,
        "primary_family": value.primary_family.value,
        "schema_version": value.schema_version,
        "source_event_id": value.source_event_id,
        "source_protocol_version": value.source_protocol_version,
        "source_replay_manifest_sha256": value.source_replay_manifest_sha256,
        "source_row_sha256": value.source_row_sha256,
        "source_rule_version": value.source_rule_version,
        "split": value.split,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_hash:
        document["anchor_sha256"] = value.anchor_sha256
    return document


def _execution_document(
    value: HistoricalExecutionContractV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "directional_vote": value.directional_vote,
        "fee_bps_per_side": value.fee_bps_per_side,
        "funding_in_decision_context": value.funding_in_decision_context,
        "interval": value.interval,
        "order_placement": value.order_placement,
        "outcome_data_used": value.outcome_data_used,
        "public_market_data_only": value.public_market_data_only,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "selection_gate_applied": value.selection_gate_applied,
        "slippage_bps_per_side": value.slippage_bps_per_side,
        "venue": value.venue.value,
        "zero_move_round_trip_cost_micros": value.zero_move_round_trip_cost_micros,
    }
    if include_hash:
        document["execution_contract_sha256"] = value.execution_contract_sha256
    return document


def _leaf_document(
    value: HistoricalDirectionalLeafV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "absolute_market_direction": value.absolute_market_direction,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "direction": value.direction,
        "family": value.family.value,
        "historical_slice_through_ms": value.historical_slice_through_ms,
        "interval": value.interval,
        "outcome_data_used": value.outcome_data_used,
        "primary_direction_used": value.primary_direction_used,
        "probability": value.probability,
        "promoting": value.promoting,
        "reasons": list(value.reasons),
        "schema_version": value.schema_version,
        "source_payload_sha256": value.source_payload_sha256,
        "source_representation": value.source_representation.value,
        "source_rule_version": value.source_rule_version,
        "source_slice_sha256": value.source_slice_sha256,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_hash:
        document["leaf_sha256"] = value.leaf_sha256
    return document


def _cost_document(
    value: HistoricalCostSurvivalContextV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "anchor_sha256": value.anchor_sha256,
        "atr_fraction_micros": value.atr_fraction_micros,
        "directional_vote": value.directional_vote,
        "execution_contract_sha256": value.execution_contract_sha256,
        "funding_included": value.funding_included,
        "one_atr_cost_headroom_micros": value.one_atr_cost_headroom_micros,
        "outcome_data_used": value.outcome_data_used,
        "schema_version": value.schema_version,
        "selection_gate_applied": value.selection_gate_applied,
        "status": value.status.value,
        "zero_move_round_trip_cost_micros": value.zero_move_round_trip_cost_micros,
    }
    if include_hash:
        document["context_sha256"] = value.context_sha256
    return document


def _consensus_document(
    value: HistoricalThreeFamilyConsensusV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "admitted": value.admitted,
        "anchor": _anchor_document(value.anchor, True),
        "bearish_family_count": value.bearish_family_count,
        "bullish_family_count": value.bullish_family_count,
        "changes_source_decision": value.changes_source_decision,
        "cost_context": _cost_document(value.cost_context, True),
        "cross_peer_input_sha256": value.cross_peer_input_sha256,
        "cross_peer_path_sha256s": [
            {"path_sha256": path_sha256, "symbol": symbol}
            for symbol, path_sha256 in value.cross_peer_path_sha256s
        ],
        "cross_peer_set_root_sha256": value.cross_peer_set_root_sha256,
        "cross_peer_symbols": list(value.cross_peer_symbols),
        "deployment_approved": value.deployment_approved,
        "directional_agreement_micros": value.directional_agreement_micros,
        "directional_denominator": value.directional_denominator,
        "directional_numerator_micros": value.directional_numerator_micros,
        "event_id": value.event_id,
        "execution_contract": _execution_document(value.execution_contract, True),
        "experiment_contract_sha256": value.experiment_contract_sha256,
        "historical_diagnostic_only": value.historical_diagnostic_only,
        "historical_only": value.historical_only,
        "invalidation": value.invalidation,
        "leaves": [_leaf_document(leaf, True) for leaf in value.leaves],
        "neutral_family_count": value.neutral_family_count,
        "order_instruction": value.order_instruction,
        "order_placement": value.order_placement,
        "outcome_data_used": value.outcome_data_used,
        "paper_executable": value.paper_executable,
        "primary_relationship": value.primary_relationship.value,
        "probability": value.probability,
        "probability_calibrated": value.probability_calibrated,
        "promoting": value.promoting,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "shadow_only": value.shadow_only,
        "source_authority_status": value.source_authority_status,
        "state_class": value.state_class.value,
        "status": value.status.value,
    }
    if include_hash:
        document["payload_sha256"] = value.payload_sha256
    return document


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 24:
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    if any(
        type(value) is not str
        or not value
        or len(value) > 256
        or not _REASON_RE.fullmatch(value)
        for value in values
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            "reasons must contain bounded canonical uppercase tokens"
        )


def _validate_sha256(value: object, label: str) -> None:
    if type(value) is not str or not _SHA256_RE.fullmatch(value):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            f"{label} must be exactly 64 lowercase hexadecimal characters"
        )


def _validate_symbol(value: object, label: str) -> None:
    if type(value) is not str or not _SYMBOL_RE.fullmatch(value):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            f"{label} must be a canonical uppercase USDT symbol"
        )


def _validate_nonnegative_int(value: object, label: str) -> None:
    if (
        type(value) is not int
        or value < 0
        or value > _MAX_CANONICAL_INTEGER
    ):
        raise HistoricalThreeFamilyConsensusContractErrorV2(
            f"{label} must be a nonnegative canonical integer"
        )


def _is_positive_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0
