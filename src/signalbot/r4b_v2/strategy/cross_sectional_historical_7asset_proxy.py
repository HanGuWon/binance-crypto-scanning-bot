from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import ROUND_FLOOR, Decimal, DecimalException, localcontext
from enum import StrEnum
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.features import ROBUST_Z_MAD_SCALE_V2
from signalbot.r4b_v2.strategy.evidence_producer import EVIDENCE_STRENGTH_SCALE_V2
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FAMILY_C_PRIOR_WINDOW_V2,
    FIVE_MINUTE_MS_V2,
    FamilyCClosedCandleV2,
)

HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2: Final = 6
HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2: Final = (
    "R4B_HISTORICAL_V2.1.0_EXACT_7_ASSET_TARGET_EXCLUDED_PROXY_"
    "SHADOW_NONPROMOTING"
)
HISTORICAL_CROSS_SECTIONAL_7ASSET_ROLE_V2: Final = (
    "HISTORICAL_ONLY_EXACT_7_ASSET_TARGET_EXCLUDED_DIRECTIONAL_PROXY"
)

_SCHEMA_VERSION: Final = "r4b_historical_cross_sectional_7asset_proxy_v2"
_SOURCE_AUTHORITY_STATUS: Final = (
    "HISTORICAL_EXACT_7_ASSET_PROXY_LIVE_M0_M1_M2_UNBOUND"
)
_INVALIDATION: Final = (
    "HISTORICAL_PROXY_CANNOT_OPEN_CLOSE_FILTER_RANK_OR_PROMOTE_LIVE_POSITIONS"
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_PATH_DOMAIN: Final = b"R4B_HISTORICAL_7ASSET_PEER_PATH_V2\0"
_INPUT_DOMAIN: Final = b"R4B_HISTORICAL_7ASSET_PROXY_INPUT_V2\0"
_EVENT_ID_DOMAIN: Final = b"R4B_HISTORICAL_7ASSET_PROXY_ID_V2\0"
_PROXY_DOMAIN: Final = b"R4B_HISTORICAL_7ASSET_PROXY_V2\0"
_CALCULATION_DOMAIN: Final = b"R4B_HISTORICAL_7ASSET_NUMERIC_CALCULATION_V2\0"
_CALCULATION_SCHEMA_VERSION: Final = (
    "r4b_historical_cross_sectional_7asset_numeric_calculation_v2"
)
_CALCULATION_ROLE: Final = (
    "HISTORICAL_ONLY_EXACT_7_ASSET_TARGET_EXCLUDED_NUMERIC_CALCULATION"
)
_STRENGTH_SCALE: Final = Decimal(EVIDENCE_STRENGTH_SCALE_V2)
_FACTORY_TOKEN: Final = object()
_CALCULATION_FACTORY_TOKEN: Final = object()


class HistoricalCrossSectional7AssetProxyContractErrorV2(ValueError):
    """Raised when the exact historical seven-asset proxy contract is broken."""


class HistoricalCrossSectional7AssetProxyStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    DATA_INVALID_ARITHMETIC = "DATA_INVALID_ARITHMETIC"


@dataclass(frozen=True, slots=True)
class HistoricalPeerCandlePathV2:
    """One immutable exact 8,644-row USD-M 5m peer path through D."""

    symbol: str
    venue: VenueV2
    candles: tuple[FamilyCClosedCandleV2, ...] = field(repr=False)
    path_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_symbol(self.symbol, "peer symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "historical peer paths require USD-M Futures"
            )
        _validate_exact_candle_path(self.symbol, self.candles)
        object.__setattr__(
            self,
            "path_sha256",
            hashlib.sha256(
                _PATH_DOMAIN + canonical_json_line(_path_document(self))
            ).hexdigest(),
        )

    @property
    def final_bar_open_ms(self) -> int:
        return self.candles[-1].bar_open_ms

    @property
    def final_bar_close_ms(self) -> int:
        return self.candles[-1].bar_close_ms


@dataclass(frozen=True, slots=True)
class HistoricalCrossSectional7AssetProxyInputV2:
    """Target identity plus exactly six immutable, target-excluded peer paths."""

    target_symbol: str
    peer_paths: tuple[HistoricalPeerCandlePathV2, ...] = field(repr=False)
    peer_symbols: tuple[str, ...] = field(init=False)
    final_decision_bar_open_ms: int = field(init=False)
    final_decision_bar_close_ms: int = field(init=False)
    input_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_symbol(self.target_symbol, "target_symbol")
        if type(self.peer_paths) is not tuple:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "peer_paths must be an immutable tuple"
            )
        if len(self.peer_paths) != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "an exact seven-asset proxy requires exactly 6 peer paths"
            )
        if any(type(path) is not HistoricalPeerCandlePathV2 for path in self.peer_paths):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "peer_paths contains an unsupported path value"
            )
        for path in self.peer_paths:
            _validate_peer_path(path)
        ordered = tuple(
            sorted(self.peer_paths, key=lambda path: path.symbol.encode("utf-8"))
        )
        peer_symbols = tuple(path.symbol for path in ordered)
        if len(set(peer_symbols)) != len(peer_symbols):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "peer_paths cannot contain duplicate symbols"
            )
        if self.target_symbol in peer_symbols:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "target candles must not be accepted as peer input"
            )
        final_slots = {
            (path.final_bar_open_ms, path.final_bar_close_ms) for path in ordered
        }
        if len(final_slots) != 1:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "all peer paths must end at the same final decision bar"
            )
        final_open_ms, final_close_ms = next(iter(final_slots))
        object.__setattr__(self, "peer_paths", ordered)
        object.__setattr__(self, "peer_symbols", peer_symbols)
        object.__setattr__(self, "final_decision_bar_open_ms", final_open_ms)
        object.__setattr__(self, "final_decision_bar_close_ms", final_close_ms)
        object.__setattr__(
            self,
            "input_sha256",
            hashlib.sha256(
                _INPUT_DOMAIN + canonical_json_line(_input_document(self))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class HistoricalCrossSectional7AssetCalculationV2:
    """Factory-sealed source-neutral arithmetic for the historical proxy.

    The value carries only the result of an exact 8,640-prior/six-current
    return calculation.  It deliberately grants no raw-data, causal-finality,
    live-readiness, efficacy, probability, or promotion authority.
    """

    status: HistoricalCrossSectional7AssetProxyStatusV2
    reasons: tuple[str, ...]
    prior_observation_count: int
    current_peer_count: int
    m3_ex_target: Decimal | None
    shock_scale: Decimal | None
    shock_score: Decimal | None
    breadth_count: int | None
    breadth_denominator: int | None
    shock_magnitude: Decimal | None
    breadth_support: Decimal | None
    direction: int
    strength_micros: int
    signed_strength_micros: int
    _factory_token: InitVar[object | None] = None
    calculation_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_CALCULATION_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2,
    )
    role: str = field(init=False, default=_CALCULATION_ROLE)
    historical_only: Literal[True] = field(init=False, default=True)
    numeric_only: Literal[True] = field(init=False, default=True)
    live_authority: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CALCULATION_FACTORY_TOKEN:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "historical cross-sectional calculation requires its sealed factory"
            )
        _validate_calculation(self)
        object.__setattr__(
            self,
            "calculation_sha256",
            hashlib.sha256(
                _CALCULATION_DOMAIN
                + canonical_json_line(
                    _calculation_document(self, include_calculation_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is HistoricalCrossSectional7AssetProxyStatusV2.READY


@dataclass(frozen=True, slots=True)
class HistoricalCrossSectional7AssetProxyV2:
    """Factory-sealed historical proxy with no live or efficacy authority."""

    source_input: HistoricalCrossSectional7AssetProxyInputV2 = field(repr=False)
    status: HistoricalCrossSectional7AssetProxyStatusV2
    reasons: tuple[str, ...]
    m3_ex_target: Decimal | None
    shock_scale: Decimal | None
    shock_score: Decimal | None
    breadth_count: int | None
    breadth_denominator: int | None
    shock_magnitude: Decimal | None
    breadth_support: Decimal | None
    direction: int
    strength_micros: int
    signed_strength_micros: int
    _calculation: InitVar[HistoricalCrossSectional7AssetCalculationV2 | None] = None
    _factory_token: InitVar[object | None] = None
    calculation_sha256: str = field(init=False)
    event_id: str = field(init=False)
    proxy_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2,
    )
    role: str = field(
        init=False,
        default=HISTORICAL_CROSS_SECTIONAL_7ASSET_ROLE_V2,
    )
    source_authority_status: str = field(
        init=False,
        default=_SOURCE_AUTHORITY_STATUS,
    )
    invalidation: str = field(init=False, default=_INVALIDATION)
    historical_only: Literal[True] = field(init=False, default=True)
    shadow_only: Literal[True] = field(init=False, default=True)
    live_authority: Literal[False] = field(init=False, default=False)
    verified_raw_membership_m0_bound: Literal[False] = field(
        init=False,
        default=False,
    )
    strict_source_parser_m1_bound: Literal[False] = field(
        init=False,
        default=False,
    )
    causal_cursor_finality_m2_bound: Literal[False] = field(
        init=False,
        default=False,
    )
    causal_inputs_complete: Literal[False] = field(init=False, default=False)
    producer_ready: Literal[False] = field(init=False, default=False)
    paper_executable: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    deployment_approved: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    target_candles_used: Literal[False] = field(init=False, default=False)
    target_return_used: Literal[False] = field(init=False, default=False)
    primary_direction_used: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)

    def __post_init__(
        self,
        _calculation: HistoricalCrossSectional7AssetCalculationV2 | None,
        _factory_token: object | None,
    ) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "historical cross-sectional proxies require their sealed factory"
            )
        if type(_calculation) is not HistoricalCrossSectional7AssetCalculationV2:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "historical cross-sectional proxy requires its sealed calculation"
            )
        canonical_historical_cross_sectional_7asset_calculation_v2(_calculation)
        object.__setattr__(
            self,
            "calculation_sha256",
            _calculation.calculation_sha256,
        )
        _validate_proxy(self, rederive=False)
        object.__setattr__(self, "event_id", _event_id(self))
        object.__setattr__(
            self,
            "proxy_sha256",
            hashlib.sha256(
                _PROXY_DOMAIN
                + canonical_json_line(_proxy_document(self, include_proxy_hash=False))
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is HistoricalCrossSectional7AssetProxyStatusV2.READY

    @property
    def target_symbol(self) -> str:
        return self.source_input.target_symbol

    @property
    def peer_symbols(self) -> tuple[str, ...]:
        return self.source_input.peer_symbols


def select_exact_historical_peer_candle_path_v2(
    *,
    symbol: str,
    venue: VenueV2,
    candles: tuple[FamilyCClosedCandleV2, ...],
    final_decision_bar_open_ms: int,
) -> HistoricalPeerCandlePathV2:
    """Select only the exact causal window; rows after D cannot affect the path."""

    _validate_symbol(symbol, "peer symbol")
    if venue is not VenueV2.USDM_FUTURES:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical peer paths require USD-M Futures"
        )
    if type(candles) is not tuple:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "source candles must be an immutable tuple"
        )
    if any(type(candle) is not FamilyCClosedCandleV2 for candle in candles):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "source candles contains an unsupported value"
        )
    _validate_nonnegative_int(final_decision_bar_open_ms, "final_decision_bar_open_ms")
    if final_decision_bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "final decision bar must align to a 5m UTC boundary"
        )
    first_open_ms = final_decision_bar_open_ms - (
        (FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
    )
    selected = tuple(
        candle
        for candle in candles
        if first_open_ms <= candle.bar_open_ms <= final_decision_bar_open_ms
    )
    return HistoricalPeerCandlePathV2(
        symbol=symbol,
        venue=venue,
        candles=selected,
    )


def build_historical_cross_sectional_7asset_proxy_v2(
    source_input: HistoricalCrossSectional7AssetProxyInputV2,
) -> HistoricalCrossSectional7AssetProxyV2:
    """Build the exact-six-peer historical shadow proxy without target returns."""

    _validate_source_input(source_input)
    derived = _derive_proxy(source_input)
    return HistoricalCrossSectional7AssetProxyV2(
        source_input=source_input,
        status=derived.status,
        reasons=derived.reasons,
        m3_ex_target=derived.m3_ex_target,
        shock_scale=derived.shock_scale,
        shock_score=derived.shock_score,
        breadth_count=derived.breadth_count,
        breadth_denominator=derived.breadth_denominator,
        shock_magnitude=derived.shock_magnitude,
        breadth_support=derived.breadth_support,
        direction=derived.direction,
        strength_micros=derived.strength_micros,
        signed_strength_micros=derived.signed_strength_micros,
        _calculation=derived,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_historical_cross_sectional_7asset_proxy_v2(
    value: HistoricalCrossSectional7AssetProxyV2,
) -> bytes:
    """Validate and serialize one sealed historical proxy."""

    if type(value) is not HistoricalCrossSectional7AssetProxyV2:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "value must be an exact HistoricalCrossSectional7AssetProxyV2"
        )
    _validate_proxy(value, rederive=True)
    if value.event_id != _event_id(value):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical proxy event ID differs"
        )
    payload = canonical_json_line(_proxy_document(value, include_proxy_hash=False))
    if value.proxy_sha256 != hashlib.sha256(_PROXY_DOMAIN + payload).hexdigest():
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical proxy hash differs from canonical content"
        )
    return canonical_json_line(_proxy_document(value, include_proxy_hash=True))


def calculate_historical_cross_sectional_7asset_returns_v2(
    *,
    prior_market_median_returns_3: tuple[Decimal, ...],
    current_peer_returns_3: tuple[Decimal, ...],
) -> HistoricalCrossSectional7AssetCalculationV2:
    """Calculate the historical ex-target proxy from precomputed log returns.

    The prior series is exactly 8,640 market-median three-bar returns.  Current
    input is an order-invariant multiset of exactly six peer three-bar returns.
    """

    if (
        type(prior_market_median_returns_3) is not tuple
        or len(prior_market_median_returns_3) != FAMILY_C_PRIOR_WINDOW_V2
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical cross calculation requires exactly 8,640 immutable "
            "prior market-median 3-bar returns"
        )
    if (
        type(current_peer_returns_3) is not tuple
        or len(current_peer_returns_3)
        != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical cross calculation requires exactly 6 immutable "
            "current peer 3-bar returns"
        )
    if any(
        not _is_finite_decimal(value)
        for series in (
            prior_market_median_returns_3,
            current_peer_returns_3,
        )
        for value in series
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical cross calculation returns must be finite Decimal values"
        )

    current_multiset = tuple(sorted(current_peer_returns_3))
    try:
        with localcontext(protocol_decimal_context_v2()):
            m3_current = _median_decimal(current_multiset)
            shock_scale = ROBUST_Z_MAD_SCALE_V2 * _mad_decimal(
                prior_market_median_returns_3
            )
            if shock_scale <= 0:
                return _not_ready_zero_scale()
            shock_score = abs(m3_current) / shock_scale
            direction = _sign(m3_current)
            breadth_count = sum(
                (direction > 0 and value > 0)
                or (direction < 0 and value < 0)
                for value in current_multiset
            )
            breadth_support = Decimal(breadth_count) / Decimal(
                HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
            )
            shock_magnitude = shock_score / (Decimal(1) + shock_score)
            strength = int(
                (
                    _STRENGTH_SCALE * shock_magnitude * breadth_support
                ).to_integral_value(rounding=ROUND_FLOOR)
            )
    except DecimalException:
        return _invalid_arithmetic()
    return HistoricalCrossSectional7AssetCalculationV2(
        status=HistoricalCrossSectional7AssetProxyStatusV2.READY,
        reasons=(
            "HISTORICAL_EXACT_7_ASSET_TARGET_EXCLUDED_PROXY_READY",
            "TARGET_CANDLES_AND_TARGET_RETURNS_STRUCTURALLY_EXCLUDED",
            "DIRECTION_IS_M3_EX_TARGET_SIGN_INDEPENDENT_OF_STRENGTH_QUANTIZATION",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        current_peer_count=HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
        m3_ex_target=m3_current,
        shock_scale=shock_scale,
        shock_score=shock_score,
        breadth_count=breadth_count,
        breadth_denominator=HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
        shock_magnitude=shock_magnitude,
        breadth_support=breadth_support,
        direction=direction,
        strength_micros=strength,
        signed_strength_micros=direction * strength,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def canonical_historical_cross_sectional_7asset_calculation_v2(
    value: HistoricalCrossSectional7AssetCalculationV2,
) -> bytes:
    """Validate and serialize one source-neutral historical calculation."""

    if type(value) is not HistoricalCrossSectional7AssetCalculationV2:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "value must be an exact HistoricalCrossSectional7AssetCalculationV2"
        )
    _validate_calculation(value)
    payload = canonical_json_line(
        _calculation_document(value, include_calculation_hash=False)
    )
    if value.calculation_sha256 != hashlib.sha256(
        _CALCULATION_DOMAIN + payload
    ).hexdigest():
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical cross calculation hash differs from canonical content"
        )
    return canonical_json_line(
        _calculation_document(value, include_calculation_hash=True)
    )


def _derive_proxy(
    source: HistoricalCrossSectional7AssetProxyInputV2,
) -> HistoricalCrossSectional7AssetCalculationV2:
    prior_rows: list[list[Decimal]] = [
        [] for _ in range(FAMILY_C_PRIOR_WINDOW_V2)
    ]
    current_returns: list[Decimal] = []
    try:
        with localcontext(protocol_decimal_context_v2()):
            for path in source.peer_paths:
                closes = tuple(candle.close for candle in path.candles)
                prior = tuple(
                    (closes[index] / closes[index - 3]).ln()
                    for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1)
                )
                if len(prior) != FAMILY_C_PRIOR_WINDOW_V2:
                    raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                        "peer prior return window is not exactly 8,640"
                    )
                for index, value in enumerate(prior):
                    prior_rows[index].append(value)
                current_returns.append((closes[-1] / closes[-4]).ln())
            prior_m3 = tuple(_median_decimal(tuple(row)) for row in prior_rows)
    except DecimalException:
        return _invalid_arithmetic()
    return calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_m3,
        current_peer_returns_3=tuple(current_returns),
    )


def _not_ready_zero_scale() -> HistoricalCrossSectional7AssetCalculationV2:
    return HistoricalCrossSectional7AssetCalculationV2(
        status=(
            HistoricalCrossSectional7AssetProxyStatusV2.FEATURE_NOT_READY_ZERO_SCALE
        ),
        reasons=(
            "HISTORICAL_TARGET_EXCLUDED_SHOCK_MAD_LE_ZERO",
            "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        current_peer_count=HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
        m3_ex_target=None,
        shock_scale=None,
        shock_score=None,
        breadth_count=None,
        breadth_denominator=None,
        shock_magnitude=None,
        breadth_support=None,
        direction=0,
        strength_micros=0,
        signed_strength_micros=0,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def _invalid_arithmetic() -> HistoricalCrossSectional7AssetCalculationV2:
    return HistoricalCrossSectional7AssetCalculationV2(
        status=HistoricalCrossSectional7AssetProxyStatusV2.DATA_INVALID_ARITHMETIC,
        reasons=(
            "HISTORICAL_TARGET_EXCLUDED_DECIMAL_ARITHMETIC_INVALID",
            "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        current_peer_count=HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
        m3_ex_target=None,
        shock_scale=None,
        shock_score=None,
        breadth_count=None,
        breadth_denominator=None,
        shock_magnitude=None,
        breadth_support=None,
        direction=0,
        strength_micros=0,
        signed_strength_micros=0,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def _validate_proxy(
    value: HistoricalCrossSectional7AssetProxyV2,
    *,
    rederive: bool,
) -> None:
    _validate_source_input(value.source_input)
    _validate_sha256(value.calculation_sha256, "calculation_sha256")
    if (
        value.schema_version != _SCHEMA_VERSION
        or value.rule_version != HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2
        or value.role != HISTORICAL_CROSS_SECTIONAL_7ASSET_ROLE_V2
        or value.source_authority_status != _SOURCE_AUTHORITY_STATUS
        or value.invalidation != _INVALIDATION
        or value.historical_only is not True
        or value.shadow_only is not True
        or value.live_authority is not False
        or value.verified_raw_membership_m0_bound is not False
        or value.strict_source_parser_m1_bound is not False
        or value.causal_cursor_finality_m2_bound is not False
        or value.causal_inputs_complete is not False
        or value.producer_ready is not False
        or value.paper_executable is not False
        or value.promoting is not False
        or value.deployment_approved is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.target_candles_used is not False
        or value.target_return_used is not False
        or value.primary_direction_used is not False
        or value.outcome_used is not False
        or value.data_through_ms is not None
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical proxy authority, use, or non-promotion flags differ"
        )
    if not isinstance(value.status, HistoricalCrossSectional7AssetProxyStatusV2):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "status must be HistoricalCrossSectional7AssetProxyStatusV2"
        )
    _validate_reasons(value.reasons)
    if type(value.direction) is not int or value.direction not in (-1, 0, 1):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "direction must be exactly -1, 0, or 1"
        )
    if (
        type(value.strength_micros) is not int
        or not 0 <= value.strength_micros <= EVIDENCE_STRENGTH_SCALE_V2
        or type(value.signed_strength_micros) is not int
        or value.signed_strength_micros
        != value.direction * value.strength_micros
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "proxy strength is outside its exact signed-magnitude contract"
        )
    numeric = (
        value.m3_ex_target,
        value.shock_scale,
        value.shock_score,
        value.breadth_count,
        value.breadth_denominator,
        value.shock_magnitude,
        value.breadth_support,
    )
    if value.status is not HistoricalCrossSectional7AssetProxyStatusV2.READY:
        if any(item is not None for item in numeric) or any(
            item != 0
            for item in (
                value.direction,
                value.strength_micros,
                value.signed_strength_micros,
            )
        ):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "non-ready proxy cannot expose partial numeric evidence"
            )
    else:
        _validate_ready_numeric(value)
    if rederive:
        expected = _derive_proxy(value.source_input)
        if (
            _derived_tuple(value) != _derived_tuple(expected)
            or value.calculation_sha256 != expected.calculation_sha256
        ):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "historical proxy fields contradict the bound source input"
            )


def _validate_calculation(
    value: HistoricalCrossSectional7AssetCalculationV2,
) -> None:
    if (
        value.schema_version != _CALCULATION_SCHEMA_VERSION
        or value.rule_version
        != HISTORICAL_CROSS_SECTIONAL_7ASSET_RULE_VERSION_V2
        or value.role != _CALCULATION_ROLE
        or value.historical_only is not True
        or value.numeric_only is not True
        or value.live_authority is not False
        or value.promoting is not False
        or value.probability is not False
        or value.outcome_used is not False
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical calculation authority or non-promotion flags differ"
        )
    if (
        value.prior_observation_count != FAMILY_C_PRIOR_WINDOW_V2
        or value.current_peer_count
        != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical calculation requires exactly 8,640 prior and 6 current returns"
        )
    if not isinstance(value.status, HistoricalCrossSectional7AssetProxyStatusV2):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "calculation status must be HistoricalCrossSectional7AssetProxyStatusV2"
        )
    _validate_reasons(value.reasons)
    if type(value.direction) is not int or value.direction not in (-1, 0, 1):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "calculation direction must be exactly -1, 0, or 1"
        )
    if (
        type(value.strength_micros) is not int
        or not 0 <= value.strength_micros <= EVIDENCE_STRENGTH_SCALE_V2
        or type(value.signed_strength_micros) is not int
        or value.signed_strength_micros
        != value.direction * value.strength_micros
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "calculation strength is outside its exact signed-magnitude contract"
        )
    expected_reasons = {
        HistoricalCrossSectional7AssetProxyStatusV2.READY: (
            "HISTORICAL_EXACT_7_ASSET_TARGET_EXCLUDED_PROXY_READY",
            "TARGET_CANDLES_AND_TARGET_RETURNS_STRUCTURALLY_EXCLUDED",
            "DIRECTION_IS_M3_EX_TARGET_SIGN_INDEPENDENT_OF_STRENGTH_QUANTIZATION",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
        HistoricalCrossSectional7AssetProxyStatusV2.FEATURE_NOT_READY_ZERO_SCALE: (
            "HISTORICAL_TARGET_EXCLUDED_SHOCK_MAD_LE_ZERO",
            "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
        HistoricalCrossSectional7AssetProxyStatusV2.DATA_INVALID_ARITHMETIC: (
            "HISTORICAL_TARGET_EXCLUDED_DECIMAL_ARITHMETIC_INVALID",
            "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK",
            "HISTORICAL_SHADOW_NONPROMOTING_NO_EFFICACY_CLAIM",
        ),
    }[value.status]
    if value.reasons != expected_reasons:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical calculation reasons differ from its status"
        )
    numeric = (
        value.m3_ex_target,
        value.shock_scale,
        value.shock_score,
        value.breadth_count,
        value.breadth_denominator,
        value.shock_magnitude,
        value.breadth_support,
    )
    if value.ready:
        _validate_ready_numeric(value)
    elif any(item is not None for item in numeric) or any(
        item != 0
        for item in (
            value.direction,
            value.strength_micros,
            value.signed_strength_micros,
        )
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "non-ready calculation cannot expose partial numeric evidence"
        )


def _validate_ready_numeric(
    value: (
        HistoricalCrossSectional7AssetProxyV2
        | HistoricalCrossSectional7AssetCalculationV2
    ),
) -> None:
    if not _is_finite_decimal(value.m3_ex_target):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY proxy requires finite m3_ex_target"
        )
    if not _is_positive_finite(value.shock_scale):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY proxy requires positive shock_scale"
        )
    if not _is_nonnegative_finite(value.shock_score):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY proxy requires nonnegative shock_score"
        )
    if not _is_nonnegative_finite(value.shock_magnitude):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY proxy requires nonnegative shock_magnitude"
        )
    if not _is_nonnegative_finite(value.breadth_support):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY proxy requires nonnegative breadth_support"
        )
    if (
        type(value.breadth_count) is not int
        or type(value.breadth_denominator) is not int
        or value.breadth_denominator
        != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
        or not 0 <= value.breadth_count <= value.breadth_denominator
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY breadth differs from the exact six-peer contract"
        )
    assert value.m3_ex_target is not None
    assert value.shock_scale is not None
    assert value.shock_score is not None
    assert value.shock_magnitude is not None
    assert value.breadth_count is not None
    assert value.breadth_denominator is not None
    assert value.breadth_support is not None
    with localcontext(protocol_decimal_context_v2()):
        if value.shock_score != abs(value.m3_ex_target) / value.shock_scale:
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "shock_score differs from abs(m3_ex_target) / shock_scale"
            )
        if value.shock_magnitude != value.shock_score / (
            Decimal(1) + value.shock_score
        ):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "shock_magnitude differs from the frozen saturating mapping"
            )
        if value.breadth_support != Decimal(value.breadth_count) / Decimal(
            value.breadth_denominator
        ):
            raise HistoricalCrossSectional7AssetProxyContractErrorV2(
                "breadth_support differs from sign-consistent peer breadth"
            )
        expected_strength = int(
            (
                _STRENGTH_SCALE
                * value.shock_magnitude
                * value.breadth_support
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
    if value.direction != _sign(value.m3_ex_target):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY direction must preserve the market shock sign"
        )
    if (
        value.strength_micros != expected_strength
        or value.signed_strength_micros != value.direction * expected_strength
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "READY strength differs from the frozen shock-by-breadth mapping"
        )


def _derived_tuple(
    value: (
        HistoricalCrossSectional7AssetProxyV2
        | HistoricalCrossSectional7AssetCalculationV2
    ),
) -> tuple[object, ...]:
    return (
        value.status,
        value.reasons,
        value.m3_ex_target,
        value.shock_scale,
        value.shock_score,
        value.breadth_count,
        value.breadth_denominator,
        value.shock_magnitude,
        value.breadth_support,
        value.direction,
        value.strength_micros,
        value.signed_strength_micros,
    )


def _validate_source_input(value: object) -> None:
    if type(value) is not HistoricalCrossSectional7AssetProxyInputV2:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "source_input must be an exact HistoricalCrossSectional7AssetProxyInputV2"
        )
    rebuilt = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=value.target_symbol,
        peer_paths=value.peer_paths,
    )
    if rebuilt != value:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "source input differs from its canonical exact-six-peer content"
        )


def _validate_peer_path(value: HistoricalPeerCandlePathV2) -> None:
    rebuilt = HistoricalPeerCandlePathV2(
        symbol=value.symbol,
        venue=value.venue,
        candles=value.candles,
    )
    if rebuilt != value:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer path differs from its canonical candle content"
        )


def _validate_exact_candle_path(
    symbol: str,
    candles: tuple[FamilyCClosedCandleV2, ...],
) -> None:
    if type(candles) is not tuple:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candles must be an immutable tuple"
        )
    if len(candles) != FAMILY_C_PANEL_BAR_COUNT_V2:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "each peer path requires exactly 8,644 candles"
        )
    if any(type(candle) is not FamilyCClosedCandleV2 for candle in candles):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candles contains an unsupported value"
        )
    final_open_ms = candles[-1].bar_open_ms
    first_open_ms = final_open_ms - (
        (FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2
    )
    expected_opens = tuple(
        first_open_ms + index * FIVE_MINUTE_MS_V2
        for index in range(FAMILY_C_PANEL_BAR_COUNT_V2)
    )
    if tuple(candle.bar_open_ms for candle in candles) != expected_opens:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle path must be ordered and contiguous through D"
        )
    for candle in candles:
        _validate_candle(candle, symbol)


def _validate_candle(candle: FamilyCClosedCandleV2, symbol: str) -> None:
    if candle.symbol != symbol:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle identity differs from its path symbol"
        )
    _validate_nonnegative_int(candle.bar_open_ms, "bar_open_ms")
    _validate_nonnegative_int(candle.bar_close_ms, "bar_close_ms")
    if candle.bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle must align to a 5m UTC boundary"
        )
    if candle.bar_close_ms != candle.bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle close differs from its 5m slot"
        )
    _validate_nonnegative_int(candle.event_time_ms, "event_time_ms")
    _validate_nonnegative_int(candle.receipt_time_ms, "receipt_time_ms")
    if candle.closed is not True:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "historical proxy accepts fully closed candles only"
        )
    if not candle.bar_open_ms <= candle.event_time_ms <= candle.bar_close_ms:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle event must stay inside its closed bar"
        )
    if candle.receipt_time_ms < candle.event_time_ms:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle receipt cannot precede its event"
        )
    if not _is_positive_finite(candle.close):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "peer candle close must be positive finite Decimal"
        )
    _validate_sha256(candle.source_evidence_sha256, "source_evidence_sha256")


def _event_id(value: HistoricalCrossSectional7AssetProxyV2) -> str:
    identity = {
        "final_decision_bar_close_ms": (
            value.source_input.final_decision_bar_close_ms
        ),
        "final_decision_bar_open_ms": value.source_input.final_decision_bar_open_ms,
        "input_sha256": value.source_input.input_sha256,
        "role": value.role,
        "rule_version": value.rule_version,
        "target_symbol": value.target_symbol,
        "venue": VenueV2.USDM_FUTURES.value,
    }
    return hashlib.sha256(
        _EVENT_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()


def _calculation_document(
    value: HistoricalCrossSectional7AssetCalculationV2,
    *,
    include_calculation_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "breadth_count": value.breadth_count,
        "breadth_denominator": value.breadth_denominator,
        "breadth_support": (
            None if value.breadth_support is None else str(value.breadth_support)
        ),
        "current_peer_count": value.current_peer_count,
        "direction": value.direction,
        "historical_only": value.historical_only,
        "live_authority": value.live_authority,
        "m3_ex_target": (
            None if value.m3_ex_target is None else str(value.m3_ex_target)
        ),
        "numeric_only": value.numeric_only,
        "outcome_used": value.outcome_used,
        "prior_observation_count": value.prior_observation_count,
        "probability": value.probability,
        "promoting": value.promoting,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "shock_magnitude": (
            None if value.shock_magnitude is None else str(value.shock_magnitude)
        ),
        "shock_scale": None if value.shock_scale is None else str(value.shock_scale),
        "shock_score": None if value.shock_score is None else str(value.shock_score),
        "signed_strength_micros": value.signed_strength_micros,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }
    if include_calculation_hash:
        document["calculation_sha256"] = value.calculation_sha256
    return document


def _proxy_document(
    value: HistoricalCrossSectional7AssetProxyV2,
    *,
    include_proxy_hash: bool,
) -> dict[str, object]:
    source = value.source_input
    document: dict[str, object] = {
        "breadth_count": value.breadth_count,
        "breadth_denominator": value.breadth_denominator,
        "breadth_support": (
            None if value.breadth_support is None else str(value.breadth_support)
        ),
        "causal_cursor_finality_m2_bound": value.causal_cursor_finality_m2_bound,
        "causal_inputs_complete": value.causal_inputs_complete,
        "data_through_ms": value.data_through_ms,
        "deployment_approved": value.deployment_approved,
        "direction": value.direction,
        "event_id": value.event_id,
        "final_decision_bar_close_ms": source.final_decision_bar_close_ms,
        "final_decision_bar_open_ms": source.final_decision_bar_open_ms,
        "historical_only": value.historical_only,
        "input_sha256": source.input_sha256,
        "invalidation": value.invalidation,
        "live_authority": value.live_authority,
        "m3_ex_target": (
            None if value.m3_ex_target is None else str(value.m3_ex_target)
        ),
        "outcome_used": value.outcome_used,
        "paper_executable": value.paper_executable,
        "peer_count": len(source.peer_paths),
        "peer_paths": [
            {"path_sha256": path.path_sha256, "symbol": path.symbol}
            for path in source.peer_paths
        ],
        "peer_symbols": list(source.peer_symbols),
        "primary_direction_used": value.primary_direction_used,
        "probability": value.probability,
        "probability_calibrated": value.probability_calibrated,
        "producer_ready": value.producer_ready,
        "promoting": value.promoting,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "shadow_only": value.shadow_only,
        "shock_magnitude": (
            None if value.shock_magnitude is None else str(value.shock_magnitude)
        ),
        "shock_scale": None if value.shock_scale is None else str(value.shock_scale),
        "shock_score": None if value.shock_score is None else str(value.shock_score),
        "signed_strength_micros": value.signed_strength_micros,
        "source_authority_status": value.source_authority_status,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "strict_source_parser_m1_bound": value.strict_source_parser_m1_bound,
        "target_candles_used": value.target_candles_used,
        "target_return_used": value.target_return_used,
        "target_symbol": source.target_symbol,
        "venue": VenueV2.USDM_FUTURES.value,
        "verified_raw_membership_m0_bound": (
            value.verified_raw_membership_m0_bound
        ),
    }
    if include_proxy_hash:
        document["proxy_sha256"] = value.proxy_sha256
    return document


def _path_document(value: HistoricalPeerCandlePathV2) -> dict[str, object]:
    return {
        "rows": [_candle_document(candle) for candle in value.candles],
        "schema_version": "r4b_historical_7asset_peer_path_v2",
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _input_document(
    value: HistoricalCrossSectional7AssetProxyInputV2,
) -> dict[str, object]:
    return {
        "final_decision_bar_close_ms": value.final_decision_bar_close_ms,
        "final_decision_bar_open_ms": value.final_decision_bar_open_ms,
        "peer_paths": [
            {"path_sha256": path.path_sha256, "symbol": path.symbol}
            for path in value.peer_paths
        ],
        "schema_version": "r4b_historical_7asset_proxy_input_v2",
        "target_symbol": value.target_symbol,
        "venue": VenueV2.USDM_FUTURES.value,
    }


def _candle_document(value: FamilyCClosedCandleV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close": str(value.close),
        "closed": value.closed,
        "event_time_ms": value.event_time_ms,
        "receipt_time_ms": value.receipt_time_ms,
        "source_evidence_sha256": value.source_evidence_sha256,
        "symbol": value.symbol,
    }


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "cross-peer median requires at least one value"
        )
    ordered = tuple(sorted(values))
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    with localcontext(protocol_decimal_context_v2()):
        return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _mad_decimal(values: tuple[Decimal, ...]) -> Decimal:
    location = _median_decimal(values)
    return _median_decimal(tuple(abs(value - location) for value in values))


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _validate_symbol(value: str, name: str) -> None:
    if type(value) is not str or _SYMBOL_RE.fullmatch(value) is None:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            f"{name} must be a normalized USDT symbol"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    if any(
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > 256
        for value in values
    ):
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            "reasons must contain bounded normalized strings"
        )


def _validate_sha256(value: str, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _validate_nonnegative_int(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise HistoricalCrossSectional7AssetProxyContractErrorV2(
            f"{name} must be a nonnegative integer"
        )


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0
