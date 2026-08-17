from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import ROUND_FLOOR, Decimal, DecimalException, localcontext
from typing import Final, TypedDict

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import (
    ROBUST_Z_PRIOR_WINDOW_V2,
    RobustZResultV2,
    RobustZStatusV2,
    robust_z_v2,
)
from signalbot.r4b_v2.strategy.closed_kline_path import (
    ClosedKlinePathContractErrorV2,
    ClosedKlinePathFailureV2,
    assess_closed_kline_path_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import EVIDENCE_STRENGTH_SCALE_V2
from signalbot.r4b_v2.strategy.family_b_features import FamilyBKlineBarV2

PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 13
PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.3.0_PRICE_R1_R12_MAD_SCALE_8640_QUANTIZED_NEUTRAL_NONPROMOTING_SHADOW"
)
PRICE_STRUCTURE_MOMENTUM_ROLE_V2: Final = "NON_PROMOTING_SHADOW_EVIDENCE"

_RETURN_START_INDEX: Final = 12
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_CLOSE_SLICE_DOMAIN: Final = b"R4B_TARGET_CLOSE_PATH_ECONOMIC_SLICE_V2\0"
_SOURCE_LINEAGE_DOMAIN: Final = b"R4B_PRICE_RAW_KLINE_LINEAGE_SHADOW_V2\0"
_EVIDENCE_DOMAIN: Final = b"R4B_PRICE_STRUCTURE_MOMENTUM_EVIDENCE_V2\0"
_CALCULATION_DOMAIN: Final = b"R4B_PRICE_CLOSE_PATH_CALCULATION_V2\0"
_FACTORY_TOKEN: Final = object()
_CALCULATION_FACTORY_TOKEN: Final = object()


class _IdentityFieldsV2(TypedDict):
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    kline_capture_root_sha256: str
    kline_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int


class PriceEvidenceContractErrorV2(ValueError):
    """Raised when price evidence violates its frozen shadow contract."""


@dataclass(frozen=True, slots=True)
class PriceClosePathCalculationV2:
    """Factory-sealed numeric result of the frozen 8,653-close calculation.

    This value owns no capture, cursor, producer-readiness, or promotion claim.
    It exists so source-specific adapters can reuse the exact frozen arithmetic
    without fabricating ``FamilyBKlineBarV2`` provenance.
    """

    status: RobustZStatusV2
    reason: str
    prior_observation_count: int
    current_return_1: Decimal | None
    current_return_12: Decimal | None
    prior_location_1: Decimal | None
    prior_location_12: Decimal | None
    prior_mad_1: Decimal | None
    prior_mad_12: Decimal | None
    prior_scale_1: Decimal | None
    prior_scale_12: Decimal | None
    normalized_return_1: Decimal | None
    normalized_return_12: Decimal | None
    composite: Decimal | None
    direction: int
    strength_micros: int
    _factory_token: InitVar[object | None] = None
    calculation_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CALCULATION_FACTORY_TOKEN:
            raise PriceEvidenceContractErrorV2(
                "price close-path calculation requires its frozen factory"
            )
        if not isinstance(self.status, RobustZStatusV2):
            raise PriceEvidenceContractErrorV2("calculation status must be RobustZStatusV2")
        _validate_reasons((self.reason,))
        if self.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2:
            raise PriceEvidenceContractErrorV2(
                "close-path calculation requires exactly 8,640 prior observations"
            )
        _validate_direction_and_strength(self.direction, self.strength_micros)
        numeric_values = _calculation_numeric_values(self)
        if self.status is RobustZStatusV2.READY:
            _validate_ready_calculation(self, numeric_values)
        elif any(value is not None for value in numeric_values):
            raise PriceEvidenceContractErrorV2(
                "non-ready close-path calculation cannot expose numeric values"
            )
        elif self.direction != 0 or self.strength_micros != 0:
            raise PriceEvidenceContractErrorV2(
                "non-ready close-path calculation must remain neutral"
            )
        object.__setattr__(
            self,
            "calculation_sha256",
            hashlib.sha256(
                _CALCULATION_DOMAIN
                + canonical_json_line(_calculation_document(self, include_calculation_hash=False))
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is RobustZStatusV2.READY


@dataclass(frozen=True, slots=True)
class PriceStructureMomentumEvidenceV2:
    """Factory-sealed R1/R12 close-path evidence without promotion authority."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    kline_capture_root_sha256: str
    kline_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    close_path_slice_sha256: str
    source_lineage_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    status: RobustZStatusV2
    reasons: tuple[str, ...]
    prior_observation_count: int
    current_return_1: Decimal | None
    current_return_12: Decimal | None
    prior_location_1: Decimal | None
    prior_location_12: Decimal | None
    prior_mad_1: Decimal | None
    prior_mad_12: Decimal | None
    prior_scale_1: Decimal | None
    prior_scale_12: Decimal | None
    normalized_return_1: Decimal | None
    normalized_return_12: Decimal | None
    composite: Decimal | None
    direction: int
    strength_micros: int
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)
    role: str = field(init=False, default=PRICE_STRUCTURE_MOMENTUM_ROLE_V2)
    rule_version: str = field(
        init=False,
        default=PRICE_STRUCTURE_MOMENTUM_RULE_VERSION_V2,
    )
    raw_membership_verified: bool = field(init=False, default=False)
    cursor_finality_verified: bool = field(init=False, default=False)
    promoting_eligible: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise PriceEvidenceContractErrorV2(
                "price evidence must be created by its causal shadow factory"
            )
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise PriceEvidenceContractErrorV2("price evidence requires USD-M Futures")
        for value, name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.kline_capture_root_sha256, "kline_capture_root_sha256"),
            (self.kline_schema_sha256, "kline_schema_sha256"),
            (self.close_path_slice_sha256, "close_path_slice_sha256"),
            (self.source_lineage_root_sha256, "source_lineage_root_sha256"),
        ):
            _validate_sha256(value, name)
        for value, name in (
            (self.bar_open_ms, "bar_open_ms"),
            (self.bar_close_ms, "bar_close_ms"),
            (self.decision_cutoff_ms, "decision_cutoff_ms"),
            (self.latest_source_event_ms, "latest_source_event_ms"),
            (self.latest_source_receipt_ms, "latest_source_receipt_ms"),
            (self.prior_observation_count, "prior_observation_count"),
        ):
            _validate_nonnegative_int(value, name)
        if (
            self.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
            or self.bar_close_ms != self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        ):
            raise PriceEvidenceContractErrorV2(
                "price decision bar must be an exact aligned 5-minute slot"
            )
        if self.decision_cutoff_ms != self.bar_close_ms + DECISION_DELAY_MS_V2:
            raise PriceEvidenceContractErrorV2("price decision cutoff must equal k.T + 2001ms")
        if not isinstance(self.status, RobustZStatusV2):
            raise PriceEvidenceContractErrorV2("status must be RobustZStatusV2")
        invalid_observation_time = (
            self.latest_source_event_ms < self.bar_close_ms
            or self.latest_source_event_ms > self.latest_source_receipt_ms
            or self.latest_source_receipt_ms > self.decision_cutoff_ms
        )
        if invalid_observation_time and self.status is not RobustZStatusV2.DATA_INVALID_FEATURE:
            raise PriceEvidenceContractErrorV2(
                "invalid kline observation time must remain DATA_INVALID_FEATURE"
            )
        _validate_reasons(self.reasons)
        _validate_direction_and_strength(self.direction, self.strength_micros)
        values = _numeric_values(self)
        if self.status is RobustZStatusV2.READY:
            self._validate_ready(values)
        elif any(value is not None for value in values):
            raise PriceEvidenceContractErrorV2(
                "non-ready price evidence cannot expose partial numeric values"
            )
        elif self.direction != 0 or self.strength_micros != 0:
            raise PriceEvidenceContractErrorV2("non-ready price evidence must remain neutral")
        object.__setattr__(
            self,
            "evidence_sha256",
            hashlib.sha256(
                _EVIDENCE_DOMAIN
                + canonical_json_line(_evidence_document(self, include_evidence_hash=False))
            ).hexdigest(),
        )

    def _validate_ready(self, values: tuple[Decimal | None, ...]) -> None:
        if self.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2:
            raise PriceEvidenceContractErrorV2(
                "READY price evidence requires exactly 8,640 prior observations"
            )
        if any(not _is_finite_decimal(value) for value in values):
            raise PriceEvidenceContractErrorV2("READY price evidence requires finite price scalars")
        assert self.prior_mad_1 is not None
        assert self.prior_mad_12 is not None
        assert self.prior_scale_1 is not None
        assert self.prior_scale_12 is not None
        assert self.composite is not None
        if (
            self.prior_mad_1 <= 0
            or self.prior_mad_12 <= 0
            or self.prior_scale_1 <= 0
            or self.prior_scale_12 <= 0
        ):
            raise PriceEvidenceContractErrorV2(
                "READY price evidence requires positive prior MAD scales"
            )
        expected_strength = _strength_micros(self.composite)
        expected_direction = _sign(self.composite) if expected_strength > 0 else 0
        if self.direction != expected_direction or self.strength_micros != expected_strength:
            raise PriceEvidenceContractErrorV2(
                "price direction or strength contradicts the frozen composite"
            )

    @property
    def ready(self) -> bool:
        return self.status is RobustZStatusV2.READY


def build_price_structure_momentum_evidence_v2(
    kline_bars: tuple[FamilyBKlineBarV2, ...],
) -> PriceStructureMomentumEvidenceV2:
    """Build the frozen 8,653-row R1/R12 non-promoting shadow evidence."""

    if type(kline_bars) is not tuple or not kline_bars:
        raise PriceEvidenceContractErrorV2("kline_bars must be a non-empty immutable tuple")
    if any(not isinstance(value, FamilyBKlineBarV2) for value in kline_bars):
        raise PriceEvidenceContractErrorV2("kline_bars contains an unsupported row")
    if len(kline_bars) > PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2:
        raise PriceEvidenceContractErrorV2("price kline history exceeds the exact 8,653-row window")
    try:
        path = assess_closed_kline_path_v2(
            kline_bars,
            maximum_rows=PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
        )
    except ClosedKlinePathContractErrorV2 as exc:
        raise PriceEvidenceContractErrorV2(str(exc)) from exc
    current = path.current
    close_slice_sha256 = _close_slice_root(path.rows, current)
    source_lineage_root_sha256 = _source_lineage_root(
        current,
        path.canonical_rows,
    )
    prior_count = max(min(len(path.rows) - 13, ROBUST_Z_PRIOR_WINDOW_V2), 0)
    if path.failure is not None:
        return _nonready(
            current,
            close_slice_sha256=close_slice_sha256,
            source_lineage_root_sha256=source_lineage_root_sha256,
            latest_event_ms=path.latest_event_ms,
            latest_receipt_ms=path.latest_receipt_ms,
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            reason=_price_failure_reason(path.failure),
            prior_count=prior_count,
        )
    if len(path.rows) < PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2:
        return _nonready(
            current,
            close_slice_sha256=close_slice_sha256,
            source_lineage_root_sha256=source_lineage_root_sha256,
            latest_event_ms=path.latest_event_ms,
            latest_receipt_ms=path.latest_receipt_ms,
            status=RobustZStatusV2.FEATURE_NOT_READY_WARMUP,
            reason="PRICE_EXACT_8653_CLOSED_KLINE_HISTORY_REQUIRED",
            prior_count=prior_count,
        )
    calculation = calculate_price_close_path_v2(tuple(value.close for value in path.rows))
    if not calculation.ready:
        return _nonready(
            current,
            close_slice_sha256=close_slice_sha256,
            source_lineage_root_sha256=source_lineage_root_sha256,
            latest_event_ms=path.latest_event_ms,
            latest_receipt_ms=path.latest_receipt_ms,
            status=calculation.status,
            reason=calculation.reason,
            prior_count=calculation.prior_observation_count,
        )
    return _ready(
        current,
        close_slice_sha256=close_slice_sha256,
        source_lineage_root_sha256=source_lineage_root_sha256,
        latest_event_ms=path.latest_event_ms,
        latest_receipt_ms=path.latest_receipt_ms,
        calculation=calculation,
    )


def calculate_price_close_path_v2(
    closes: tuple[Decimal, ...],
) -> PriceClosePathCalculationV2:
    """Run the frozen price calculation on exactly 8,653 ordered closes."""

    if type(closes) is not tuple or len(closes) != PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2:
        raise PriceEvidenceContractErrorV2(
            "price calculation requires exactly 8,653 immutable closes"
        )
    if any(not _is_finite_decimal(value) or value <= 0 for value in closes):
        raise PriceEvidenceContractErrorV2(
            "price calculation closes must be positive finite Decimal values"
        )
    try:
        returns_1, returns_12 = _return_series_from_closes(closes)
        return calculate_price_return_series_v2(returns_1, returns_12)
    except DecimalException:
        return _nonready_calculation(
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            reason="PRICE_DECIMAL_ARITHMETIC_INVALID",
        )


def calculate_price_return_series_v2(
    returns_1: tuple[Decimal, ...],
    returns_12: tuple[Decimal, ...],
) -> PriceClosePathCalculationV2:
    """Run the frozen price calculation on precomputed R1/R12 log returns.

    Each immutable series contains the same exact 8,640 prior observations and
    one current observation produced by the 8,653-close owner.  This numeric
    boundary carries no capture, finality, live-readiness, or promotion claim.
    """

    expected = ROBUST_Z_PRIOR_WINDOW_V2 + 1
    if (
        type(returns_1) is not tuple
        or type(returns_12) is not tuple
        or len(returns_1) != expected
        or len(returns_12) != expected
    ):
        raise PriceEvidenceContractErrorV2(
            "price return calculation requires exactly 8,641 immutable "
            "R1 and R12 returns"
        )
    if any(
        not _is_finite_decimal(value)
        for series in (returns_1, returns_12)
        for value in series
    ):
        raise PriceEvidenceContractErrorV2(
            "price return calculation requires finite Decimal values"
        )
    try:
        result_1 = robust_z_v2(returns_1[:-1], returns_1[-1])
        result_12 = robust_z_v2(returns_12[:-1], returns_12[-1])
        status = _combined_status(result_1, result_12)
        if status is not RobustZStatusV2.READY:
            return _nonready_calculation(
                status=status,
                reason=f"PRICE_R1_R12_{status.value}",
            )
        return _ready_calculation(
            current_return_1=returns_1[-1],
            current_return_12=returns_12[-1],
            result_1=result_1,
            result_12=result_12,
        )
    except DecimalException:
        return _nonready_calculation(
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            reason="PRICE_DECIMAL_ARITHMETIC_INVALID",
        )


def canonical_price_close_path_calculation_v2(
    calculation: PriceClosePathCalculationV2,
) -> bytes:
    """Serialize and live-validate one frozen numeric calculation."""

    if not isinstance(calculation, PriceClosePathCalculationV2):
        raise PriceEvidenceContractErrorV2("calculation must be PriceClosePathCalculationV2")
    expected = hashlib.sha256(
        _CALCULATION_DOMAIN
        + canonical_json_line(_calculation_document(calculation, include_calculation_hash=False))
    ).hexdigest()
    if calculation.calculation_sha256 != expected:
        raise PriceEvidenceContractErrorV2(
            "price close-path calculation differs from canonical content"
        )
    return canonical_json_line(_calculation_document(calculation, include_calculation_hash=True))


def canonical_price_structure_momentum_evidence_v2(
    evidence: PriceStructureMomentumEvidenceV2,
) -> bytes:
    if not isinstance(evidence, PriceStructureMomentumEvidenceV2):
        raise PriceEvidenceContractErrorV2("evidence must be PriceStructureMomentumEvidenceV2")
    expected = hashlib.sha256(
        _EVIDENCE_DOMAIN
        + canonical_json_line(_evidence_document(evidence, include_evidence_hash=False))
    ).hexdigest()
    if evidence.evidence_sha256 != expected:
        raise PriceEvidenceContractErrorV2("price evidence hash differs from canonical content")
    return canonical_json_line(_evidence_document(evidence, include_evidence_hash=True))


def _ready(
    current: FamilyBKlineBarV2,
    *,
    close_slice_sha256: str,
    source_lineage_root_sha256: str,
    latest_event_ms: int,
    latest_receipt_ms: int,
    calculation: PriceClosePathCalculationV2,
) -> PriceStructureMomentumEvidenceV2:
    assert calculation.ready
    return PriceStructureMomentumEvidenceV2(
        **_identity(current),
        close_path_slice_sha256=close_slice_sha256,
        source_lineage_root_sha256=source_lineage_root_sha256,
        latest_source_event_ms=latest_event_ms,
        latest_source_receipt_ms=latest_receipt_ms,
        status=RobustZStatusV2.READY,
        reasons=(
            "PRICE_R1_R12_PRIOR_ONLY_MAD_SCALE_READY",
            "RAW_MEMBERSHIP_AND_CURSOR_FINALITY_NOT_CONNECTED_SHADOW",
        ),
        prior_observation_count=calculation.prior_observation_count,
        current_return_1=calculation.current_return_1,
        current_return_12=calculation.current_return_12,
        prior_location_1=calculation.prior_location_1,
        prior_location_12=calculation.prior_location_12,
        prior_mad_1=calculation.prior_mad_1,
        prior_mad_12=calculation.prior_mad_12,
        prior_scale_1=calculation.prior_scale_1,
        prior_scale_12=calculation.prior_scale_12,
        normalized_return_1=calculation.normalized_return_1,
        normalized_return_12=calculation.normalized_return_12,
        composite=calculation.composite,
        direction=calculation.direction,
        strength_micros=calculation.strength_micros,
        _factory_token=_FACTORY_TOKEN,
    )


def _nonready(
    current: FamilyBKlineBarV2,
    *,
    close_slice_sha256: str,
    source_lineage_root_sha256: str,
    latest_event_ms: int,
    latest_receipt_ms: int,
    status: RobustZStatusV2,
    reason: str,
    prior_count: int,
) -> PriceStructureMomentumEvidenceV2:
    return PriceStructureMomentumEvidenceV2(
        **_identity(current),
        close_path_slice_sha256=close_slice_sha256,
        source_lineage_root_sha256=source_lineage_root_sha256,
        latest_source_event_ms=latest_event_ms,
        latest_source_receipt_ms=latest_receipt_ms,
        status=status,
        reasons=(reason, "NON_PROMOTING_SHADOW_WITHOUT_RAW_MEMBERSHIP_AUTHORITY"),
        prior_observation_count=prior_count,
        current_return_1=None,
        current_return_12=None,
        prior_location_1=None,
        prior_location_12=None,
        prior_mad_1=None,
        prior_mad_12=None,
        prior_scale_1=None,
        prior_scale_12=None,
        normalized_return_1=None,
        normalized_return_12=None,
        composite=None,
        direction=0,
        strength_micros=0,
        _factory_token=_FACTORY_TOKEN,
    )


def _return_series_from_closes(
    closes: tuple[Decimal, ...],
) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    with localcontext(protocol_decimal_context_v2()):
        returns_1 = tuple(
            (closes[index] / closes[index - 1]).ln()
            for index in range(_RETURN_START_INDEX, len(closes))
        )
        returns_12 = tuple(
            (closes[index] / closes[index - 12]).ln()
            for index in range(_RETURN_START_INDEX, len(closes))
        )
    expected = ROBUST_Z_PRIOR_WINDOW_V2 + 1
    if len(returns_1) != expected or len(returns_12) != expected:
        raise PriceEvidenceContractErrorV2(
            "exact price path did not produce 8,640 prior plus one current return"
        )
    return returns_1, returns_12


def _ready_calculation(
    *,
    current_return_1: Decimal,
    current_return_12: Decimal,
    result_1: RobustZResultV2,
    result_12: RobustZResultV2,
) -> PriceClosePathCalculationV2:
    for result in (result_1, result_12):
        assert result.location is not None
        assert result.mad is not None
        assert result.scale is not None
    assert result_1.location is not None
    assert result_1.mad is not None
    assert result_1.scale is not None
    assert result_12.location is not None
    assert result_12.mad is not None
    assert result_12.scale is not None
    with localcontext(protocol_decimal_context_v2()):
        normalized_1 = current_return_1 / result_1.scale
        normalized_12 = current_return_12 / result_12.scale
        composite = (normalized_1 + normalized_12) / Decimal(2)
    strength_micros = _strength_micros(composite)
    direction = _sign(composite) if strength_micros > 0 else 0
    return PriceClosePathCalculationV2(
        status=RobustZStatusV2.READY,
        reason="PRICE_R1_R12_PRIOR_ONLY_MAD_SCALE_READY",
        prior_observation_count=ROBUST_Z_PRIOR_WINDOW_V2,
        current_return_1=current_return_1,
        current_return_12=current_return_12,
        prior_location_1=result_1.location,
        prior_location_12=result_12.location,
        prior_mad_1=result_1.mad,
        prior_mad_12=result_12.mad,
        prior_scale_1=result_1.scale,
        prior_scale_12=result_12.scale,
        normalized_return_1=normalized_1,
        normalized_return_12=normalized_12,
        composite=composite,
        direction=direction,
        strength_micros=strength_micros,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def _nonready_calculation(
    *,
    status: RobustZStatusV2,
    reason: str,
) -> PriceClosePathCalculationV2:
    return PriceClosePathCalculationV2(
        status=status,
        reason=reason,
        prior_observation_count=ROBUST_Z_PRIOR_WINDOW_V2,
        current_return_1=None,
        current_return_12=None,
        prior_location_1=None,
        prior_location_12=None,
        prior_mad_1=None,
        prior_mad_12=None,
        prior_scale_1=None,
        prior_scale_12=None,
        normalized_return_1=None,
        normalized_return_12=None,
        composite=None,
        direction=0,
        strength_micros=0,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def _combined_status(
    result_1: RobustZResultV2,
    result_12: RobustZResultV2,
) -> RobustZStatusV2:
    statuses = (result_1.status, result_12.status)
    for status in (
        RobustZStatusV2.DATA_INVALID_FEATURE,
        RobustZStatusV2.FEATURE_NOT_READY_WARMUP,
        RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
    ):
        if status in statuses:
            return status
    return RobustZStatusV2.READY


def _identity(value: FamilyBKlineBarV2) -> _IdentityFieldsV2:
    return {
        "symbol": value.symbol,
        "venue": value.venue,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "kline_capture_root_sha256": value.capture_root_sha256,
        "kline_schema_sha256": value.schema_sha256,
        "bar_open_ms": value.bar_open_ms,
        "bar_close_ms": value.bar_close_ms,
        "decision_cutoff_ms": value.bar_close_ms + DECISION_DELAY_MS_V2,
    }


def _close_slice_root(
    rows: tuple[FamilyBKlineBarV2, ...],
    current: FamilyBKlineBarV2,
) -> str:
    return hashlib.sha256(
        _CLOSE_SLICE_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": current.bar_close_ms,
                "bar_open_ms": current.bar_open_ms,
                "promoting_plan_sha256": current.promoting_plan_sha256,
                "rows": [
                    {
                        "bar_close_ms": value.bar_close_ms,
                        "bar_open_ms": value.bar_open_ms,
                        "close": str(value.close),
                    }
                    for value in rows
                ],
                "schema_version": "r4b_target_close_path_economic_slice_v2",
                "symbol": current.symbol,
                "venue": current.venue.value,
            }
        )
    ).hexdigest()


def _source_lineage_root(
    current: FamilyBKlineBarV2,
    canonical_rows: tuple[bytes, ...],
) -> str:
    return hashlib.sha256(
        _SOURCE_LINEAGE_DOMAIN
        + canonical_json_line(
            {
                "authority_status": "RAW_MEMBERSHIP_AND_CURSOR_FINALITY_NOT_CONNECTED",
                "bar_close_ms": current.bar_close_ms,
                "bar_open_ms": current.bar_open_ms,
                "kline_capture_root_sha256": current.capture_root_sha256,
                "kline_row_sha256s": [
                    hashlib.sha256(value).hexdigest() for value in canonical_rows
                ],
                "kline_schema_sha256": current.schema_sha256,
                "promoting_plan_sha256": current.promoting_plan_sha256,
                "schema_version": "r4b_price_raw_kline_lineage_shadow_v2",
                "symbol": current.symbol,
                "venue": current.venue.value,
            }
        )
    ).hexdigest()


def _price_failure_reason(failure: ClosedKlinePathFailureV2) -> str:
    return {
        ClosedKlinePathFailureV2.IDENTITY_DRIFT: "PRICE_KLINE_IDENTITY_DRIFT",
        ClosedKlinePathFailureV2.ROW_NOT_CLOSED_EXACT_5M: (
            "PRICE_REQUIRES_CLOSED_CONTIGUOUS_5M_KLINES"
        ),
        ClosedKlinePathFailureV2.EVENT_PRECEDES_OWN_CLOSE: ("PRICE_KLINE_EVENT_PRECEDES_OWN_CLOSE"),
        ClosedKlinePathFailureV2.RECEIPT_PRECEDES_EVENT: ("PRICE_KLINE_RECEIPT_PRECEDES_EVENT"),
        ClosedKlinePathFailureV2.RECEIPT_AFTER_DECISION_CUTOFF: (
            "PRICE_KLINE_RECEIPT_AFTER_DECISION_CUTOFF"
        ),
        ClosedKlinePathFailureV2.HISTORY_GAP_OR_DUPLICATE: ("PRICE_KLINE_HISTORY_GAP_OR_DUPLICATE"),
        ClosedKlinePathFailureV2.PREVIOUS_CLOSE_CHAIN_MISMATCH: (
            "PRICE_PREVIOUS_CLOSE_CHAIN_MISMATCH"
        ),
    }[failure]


def _strength_micros(composite: Decimal) -> int:
    with localcontext(protocol_decimal_context_v2()):
        magnitude = abs(composite)
        scaled = Decimal(EVIDENCE_STRENGTH_SCALE_V2) * magnitude / (Decimal(1) + magnitude)
        return int(scaled.to_integral_value(rounding=ROUND_FLOOR))


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _numeric_values(
    value: PriceStructureMomentumEvidenceV2,
) -> tuple[Decimal | None, ...]:
    return (
        value.current_return_1,
        value.current_return_12,
        value.prior_location_1,
        value.prior_location_12,
        value.prior_mad_1,
        value.prior_mad_12,
        value.prior_scale_1,
        value.prior_scale_12,
        value.normalized_return_1,
        value.normalized_return_12,
        value.composite,
    )


def _calculation_numeric_values(
    value: PriceClosePathCalculationV2,
) -> tuple[Decimal | None, ...]:
    return (
        value.current_return_1,
        value.current_return_12,
        value.prior_location_1,
        value.prior_location_12,
        value.prior_mad_1,
        value.prior_mad_12,
        value.prior_scale_1,
        value.prior_scale_12,
        value.normalized_return_1,
        value.normalized_return_12,
        value.composite,
    )


def _validate_ready_calculation(
    value: PriceClosePathCalculationV2,
    numeric_values: tuple[Decimal | None, ...],
) -> None:
    if any(not _is_finite_decimal(item) for item in numeric_values):
        raise PriceEvidenceContractErrorV2(
            "READY close-path calculation requires finite numeric values"
        )
    assert value.current_return_1 is not None
    assert value.current_return_12 is not None
    assert value.prior_mad_1 is not None
    assert value.prior_mad_12 is not None
    assert value.prior_scale_1 is not None
    assert value.prior_scale_12 is not None
    assert value.normalized_return_1 is not None
    assert value.normalized_return_12 is not None
    assert value.composite is not None
    if (
        value.prior_mad_1 <= 0
        or value.prior_mad_12 <= 0
        or value.prior_scale_1 <= 0
        or value.prior_scale_12 <= 0
    ):
        raise PriceEvidenceContractErrorV2(
            "READY close-path calculation requires positive MAD scales"
        )
    with localcontext(protocol_decimal_context_v2()):
        expected_normalized_1 = value.current_return_1 / value.prior_scale_1
        expected_normalized_12 = value.current_return_12 / value.prior_scale_12
        expected_composite = (expected_normalized_1 + expected_normalized_12) / Decimal(2)
    expected_strength = _strength_micros(expected_composite)
    expected_direction = _sign(expected_composite) if expected_strength > 0 else 0
    if (
        value.reason != "PRICE_R1_R12_PRIOR_ONLY_MAD_SCALE_READY"
        or value.normalized_return_1 != expected_normalized_1
        or value.normalized_return_12 != expected_normalized_12
        or value.composite != expected_composite
        or value.direction != expected_direction
        or value.strength_micros != expected_strength
    ):
        raise PriceEvidenceContractErrorV2(
            "READY close-path calculation contradicts the frozen formula"
        )


def _calculation_document(
    value: PriceClosePathCalculationV2,
    *,
    include_calculation_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
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
        "schema_version": "r4b_price_close_path_calculation_v2",
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }
    if include_calculation_hash:
        document["calculation_sha256"] = value.calculation_sha256
    return document


def _evidence_document(
    value: PriceStructureMomentumEvidenceV2,
    *,
    include_evidence_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "close_path_slice_sha256": value.close_path_slice_sha256,
        "composite": _decimal_or_none(value.composite),
        "current_return_1": _decimal_or_none(value.current_return_1),
        "current_return_12": _decimal_or_none(value.current_return_12),
        "cursor_finality_verified": value.cursor_finality_verified,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "direction": value.direction,
        "kline_capture_root_sha256": value.kline_capture_root_sha256,
        "kline_schema_sha256": value.kline_schema_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "normalized_return_1": _decimal_or_none(value.normalized_return_1),
        "normalized_return_12": _decimal_or_none(value.normalized_return_12),
        "prior_location_1": _decimal_or_none(value.prior_location_1),
        "prior_location_12": _decimal_or_none(value.prior_location_12),
        "prior_mad_1": _decimal_or_none(value.prior_mad_1),
        "prior_mad_12": _decimal_or_none(value.prior_mad_12),
        "prior_observation_count": value.prior_observation_count,
        "prior_scale_1": _decimal_or_none(value.prior_scale_1),
        "prior_scale_12": _decimal_or_none(value.prior_scale_12),
        "promoting_eligible": value.promoting_eligible,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "raw_membership_verified": value.raw_membership_verified,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": "r4b_price_structure_momentum_evidence_v2",
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_evidence_hash:
        document["evidence_sha256"] = value.evidence_sha256
    return document


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _validate_direction_and_strength(direction: int, strength_micros: int) -> None:
    if type(direction) is not int or direction not in (-1, 0, 1):
        raise PriceEvidenceContractErrorV2("direction must be exactly -1, 0, or 1")
    if type(strength_micros) is not int or not 0 <= strength_micros <= EVIDENCE_STRENGTH_SCALE_V2:
        raise PriceEvidenceContractErrorV2("strength_micros must be an integer in [0, 1000000]")
    if (direction == 0) != (strength_micros == 0):
        raise PriceEvidenceContractErrorV2(
            "direction must be neutral exactly when directional strength is zero"
        )


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise PriceEvidenceContractErrorV2("symbol must be a normalized USDT symbol")


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise PriceEvidenceContractErrorV2("reasons must be a non-empty bounded immutable tuple")
    if any(
        not isinstance(value, str) or not value or value.strip() != value or len(value) > 256
        for value in values
    ):
        raise PriceEvidenceContractErrorV2("reason must be a bounded normalized identity")


def _validate_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PriceEvidenceContractErrorV2(f"{name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise PriceEvidenceContractErrorV2(f"{name} must be a nonnegative integer")


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()
