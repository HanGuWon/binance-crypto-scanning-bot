from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException, localcontext
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
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBKlineBarV2,
)

VOLATILITY_REGIME_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.3.0_VOLATILITY_REGIME_TR_8640_ANCHORED_V2_SHADOW"
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_SLICE_ROOT_DOMAIN: Final = b"R4B_VOLATILITY_REGIME_KLINE_SLICE_V2\0"
_EVIDENCE_DOMAIN: Final = b"R4B_VOLATILITY_REGIME_EVIDENCE_V2\0"
_FACTORY_TOKEN: Final = object()


class _IdentityFieldsV2(TypedDict):
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    kline_capture_root_sha256: str
    kline_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int


class VolatilityEvidenceContractErrorV2(ValueError):
    """Raised when a volatility context slice violates its causal contract."""


@dataclass(frozen=True, slots=True)
class VolatilityRegimeEvidenceV2:
    """Factory-sealed, non-directional TR regime evidence."""

    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    kline_capture_root_sha256: str
    kline_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    kline_slice_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    status: RobustZStatusV2
    reasons: tuple[str, ...]
    prior_observation_count: int
    current_true_range: Decimal | None
    robust_z: Decimal | None
    prior_location: Decimal | None
    prior_mad: Decimal | None
    prior_scale: Decimal | None
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)
    directional: bool = field(init=False, default=False)
    direction: int = field(init=False, default=0)
    directional_strength_micros: int = field(init=False, default=0)
    rule_version: str = field(init=False, default=VOLATILITY_REGIME_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise VolatilityEvidenceContractErrorV2(
                "volatility evidence must be created by its causal factory"
            )
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise VolatilityEvidenceContractErrorV2(
                "volatility evidence requires USD-M Futures"
            )
        for value, name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.kline_capture_root_sha256, "kline_capture_root_sha256"),
            (self.kline_schema_sha256, "kline_schema_sha256"),
            (self.kline_slice_sha256, "kline_slice_sha256"),
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
        if self.bar_close_ms != self.bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
            raise VolatilityEvidenceContractErrorV2(
                "volatility decision bar must be exactly 5 minutes"
            )
        if self.decision_cutoff_ms != self.bar_close_ms + DECISION_DELAY_MS_V2:
            raise VolatilityEvidenceContractErrorV2(
                "volatility decision cutoff must equal k.T + 2001ms"
            )
        if not isinstance(self.status, RobustZStatusV2):
            raise VolatilityEvidenceContractErrorV2(
                "status must be RobustZStatusV2"
            )
        invalid_observation_time = (
            self.latest_source_event_ms < self.bar_close_ms
            or self.latest_source_event_ms > self.latest_source_receipt_ms
            or self.latest_source_receipt_ms > self.decision_cutoff_ms
        )
        if (
            invalid_observation_time
            and self.status is not RobustZStatusV2.DATA_INVALID_FEATURE
        ):
            raise VolatilityEvidenceContractErrorV2(
                "invalid kline observation time must remain DATA_INVALID_FEATURE"
            )
        _validate_reasons(self.reasons)
        outputs = (
            self.current_true_range,
            self.robust_z,
            self.prior_location,
            self.prior_mad,
            self.prior_scale,
        )
        if self.status is RobustZStatusV2.READY:
            if self.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2:
                raise VolatilityEvidenceContractErrorV2(
                    "READY volatility evidence requires exactly 8,640 prior TRs"
                )
            if any(not _is_finite_decimal(value) for value in outputs):
                raise VolatilityEvidenceContractErrorV2(
                    "READY volatility evidence requires finite regime values"
                )
            assert self.current_true_range is not None
            assert self.prior_mad is not None
            assert self.prior_scale is not None
            if (
                self.current_true_range < 0
                or self.prior_mad <= 0
                or self.prior_scale <= 0
            ):
                raise VolatilityEvidenceContractErrorV2(
                    "READY volatility ranges and scales are contradictory"
                )
        elif any(value is not None for value in outputs):
            raise VolatilityEvidenceContractErrorV2(
                "non-ready volatility evidence cannot expose partial numeric values"
            )
        object.__setattr__(
            self,
            "evidence_sha256",
            hashlib.sha256(
                _EVIDENCE_DOMAIN
                + canonical_json_line(
                    _evidence_document(self, include_evidence_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is RobustZStatusV2.READY


def build_volatility_regime_evidence_v2(
    kline_bars: tuple[FamilyBKlineBarV2, ...],
) -> VolatilityRegimeEvidenceV2:
    """Build a prior-only 8,640-TR regime; never infer price direction."""

    if type(kline_bars) is not tuple or not kline_bars:
        raise VolatilityEvidenceContractErrorV2(
            "kline_bars must be a non-empty immutable tuple"
        )
    if any(not isinstance(value, FamilyBKlineBarV2) for value in kline_bars):
        raise VolatilityEvidenceContractErrorV2(
            "kline_bars contains an unsupported row"
        )
    maximum_rows = ROBUST_Z_PRIOR_WINDOW_V2 + 2
    if len(kline_bars) > maximum_rows:
        raise VolatilityEvidenceContractErrorV2(
            "volatility kline history exceeds the anchor plus exact 8,640-prior window"
        )
    try:
        path = assess_closed_kline_path_v2(
            kline_bars,
            maximum_rows=maximum_rows,
        )
    except ClosedKlinePathContractErrorV2 as exc:
        raise VolatilityEvidenceContractErrorV2(str(exc)) from exc
    ordered = path.rows
    current = path.current
    canonical_rows = path.canonical_rows
    slice_sha256 = _slice_root(current, canonical_rows)
    latest_event_ms = path.latest_event_ms
    latest_receipt_ms = path.latest_receipt_ms
    structural_reason = _volatility_failure_reason(path.failure)
    if structural_reason is not None:
        return _nonready(
            current,
            slice_sha256=slice_sha256,
            latest_event_ms=latest_event_ms,
            latest_receipt_ms=latest_receipt_ms,
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            reason=structural_reason,
            prior_count=max(len(ordered) - 2, 0),
        )
    if len(ordered) < 2:
        return _nonready(
            current,
            slice_sha256=slice_sha256,
            latest_event_ms=latest_event_ms,
            latest_receipt_ms=latest_receipt_ms,
            status=RobustZStatusV2.FEATURE_NOT_READY_WARMUP,
            reason="VOLATILITY_ROBUST_Z_FEATURE_NOT_READY_WARMUP",
            prior_count=0,
        )
    try:
        true_ranges = tuple(_true_range(value) for value in ordered[1:])
        result = robust_z_v2(true_ranges[:-1], true_ranges[-1])
    except DecimalException:
        return _nonready(
            current,
            slice_sha256=slice_sha256,
            latest_event_ms=latest_event_ms,
            latest_receipt_ms=latest_receipt_ms,
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            reason="VOLATILITY_DECIMAL_ARITHMETIC_INVALID",
            prior_count=max(len(ordered) - 2, 0),
        )
    if not result.ready:
        return _nonready(
            current,
            slice_sha256=slice_sha256,
            latest_event_ms=latest_event_ms,
            latest_receipt_ms=latest_receipt_ms,
            status=result.status,
            reason=f"VOLATILITY_ROBUST_Z_{result.status.value}",
            prior_count=result.prior_observation_count,
        )
    return _ready(
        current,
        slice_sha256=slice_sha256,
        latest_event_ms=latest_event_ms,
        latest_receipt_ms=latest_receipt_ms,
        current_true_range=true_ranges[-1],
        result=result,
    )


def canonical_volatility_regime_evidence_v2(
    evidence: VolatilityRegimeEvidenceV2,
) -> bytes:
    if not isinstance(evidence, VolatilityRegimeEvidenceV2):
        raise VolatilityEvidenceContractErrorV2(
            "evidence must be VolatilityRegimeEvidenceV2"
        )
    expected = hashlib.sha256(
        _EVIDENCE_DOMAIN
        + canonical_json_line(_evidence_document(evidence, include_evidence_hash=False))
    ).hexdigest()
    if evidence.evidence_sha256 != expected:
        raise VolatilityEvidenceContractErrorV2(
            "volatility evidence hash differs from canonical content"
        )
    return canonical_json_line(_evidence_document(evidence, include_evidence_hash=True))


def _ready(
    current: FamilyBKlineBarV2,
    *,
    slice_sha256: str,
    latest_event_ms: int,
    latest_receipt_ms: int,
    current_true_range: Decimal,
    result: RobustZResultV2,
) -> VolatilityRegimeEvidenceV2:
    assert result.value is not None
    assert result.location is not None
    assert result.mad is not None
    assert result.scale is not None
    return VolatilityRegimeEvidenceV2(
        **_identity(current),
        kline_slice_sha256=slice_sha256,
        latest_source_event_ms=latest_event_ms,
        latest_source_receipt_ms=latest_receipt_ms,
        status=RobustZStatusV2.READY,
        reasons=(
            "EXACT_8640_PRIOR_TRUE_RANGE_REGIME_WITH_IN_SLICE_ANCHOR_READY",
            "VOLATILITY_IS_NON_DIRECTIONAL",
        ),
        prior_observation_count=result.prior_observation_count,
        current_true_range=current_true_range,
        robust_z=result.value,
        prior_location=result.location,
        prior_mad=result.mad,
        prior_scale=result.scale,
        _factory_token=_FACTORY_TOKEN,
    )


def _nonready(
    current: FamilyBKlineBarV2,
    *,
    slice_sha256: str,
    latest_event_ms: int,
    latest_receipt_ms: int,
    status: RobustZStatusV2,
    reason: str,
    prior_count: int,
) -> VolatilityRegimeEvidenceV2:
    return VolatilityRegimeEvidenceV2(
        **_identity(current),
        kline_slice_sha256=slice_sha256,
        latest_source_event_ms=latest_event_ms,
        latest_source_receipt_ms=latest_receipt_ms,
        status=status,
        reasons=(reason,),
        prior_observation_count=prior_count,
        current_true_range=None,
        robust_z=None,
        prior_location=None,
        prior_mad=None,
        prior_scale=None,
        _factory_token=_FACTORY_TOKEN,
    )


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


def _volatility_failure_reason(
    failure: ClosedKlinePathFailureV2 | None,
) -> str | None:
    if failure is None:
        return None
    return {
        ClosedKlinePathFailureV2.IDENTITY_DRIFT: (
            "VOLATILITY_KLINE_IDENTITY_DRIFT"
        ),
        ClosedKlinePathFailureV2.ROW_NOT_CLOSED_EXACT_5M: (
            "VOLATILITY_REQUIRES_CLOSED_CONTIGUOUS_5M_KLINES"
        ),
        ClosedKlinePathFailureV2.EVENT_PRECEDES_OWN_CLOSE: (
            "VOLATILITY_KLINE_EVENT_PRECEDES_OWN_CLOSE"
        ),
        ClosedKlinePathFailureV2.RECEIPT_PRECEDES_EVENT: (
            "VOLATILITY_KLINE_RECEIPT_PRECEDES_EVENT"
        ),
        ClosedKlinePathFailureV2.RECEIPT_AFTER_DECISION_CUTOFF: (
            "VOLATILITY_KLINE_RECEIPT_AFTER_DECISION_CUTOFF"
        ),
        ClosedKlinePathFailureV2.HISTORY_GAP_OR_DUPLICATE: (
            "VOLATILITY_KLINE_HISTORY_HAS_GAP"
        ),
        ClosedKlinePathFailureV2.PREVIOUS_CLOSE_CHAIN_MISMATCH: (
            "VOLATILITY_PREVIOUS_CLOSE_CHAIN_MISMATCH"
        ),
    }[failure]


def _slice_root(current: FamilyBKlineBarV2, rows: tuple[bytes, ...]) -> str:
    return hashlib.sha256(
        _SLICE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": current.bar_close_ms,
                "bar_open_ms": current.bar_open_ms,
                "kline_capture_root_sha256": current.capture_root_sha256,
                "kline_row_sha256s": [
                    hashlib.sha256(value).hexdigest() for value in rows
                ],
                "kline_schema_sha256": current.schema_sha256,
                "promoting_plan_sha256": current.promoting_plan_sha256,
                "schema_version": "r4b_volatility_regime_kline_slice_v2",
                "symbol": current.symbol,
                "venue": current.venue.value,
            }
        )
    ).hexdigest()


def _true_range(value: FamilyBKlineBarV2) -> Decimal:
    with localcontext(protocol_decimal_context_v2()):
        return max(
            value.high - value.low,
            abs(value.high - value.previous_close),
            abs(value.low - value.previous_close),
        )


def _evidence_document(
    value: VolatilityRegimeEvidenceV2,
    *,
    include_evidence_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "current_true_range": (
            None if value.current_true_range is None else str(value.current_true_range)
        ),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "direction": value.direction,
        "directional": value.directional,
        "directional_strength_micros": value.directional_strength_micros,
        "kline_capture_root_sha256": value.kline_capture_root_sha256,
        "kline_schema_sha256": value.kline_schema_sha256,
        "kline_slice_sha256": value.kline_slice_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "prior_location": (
            None if value.prior_location is None else str(value.prior_location)
        ),
        "prior_mad": None if value.prior_mad is None else str(value.prior_mad),
        "prior_observation_count": value.prior_observation_count,
        "prior_scale": None if value.prior_scale is None else str(value.prior_scale),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "reasons": list(value.reasons),
        "robust_z": None if value.robust_z is None else str(value.robust_z),
        "rule_version": value.rule_version,
        "schema_version": "r4b_volatility_regime_evidence_v2",
        "status": value.status.value,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }
    if include_evidence_hash:
        document["evidence_sha256"] = value.evidence_sha256
    return document


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise VolatilityEvidenceContractErrorV2(
            "symbol must be a normalized USDT symbol"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise VolatilityEvidenceContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    if any(
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        for value in values
    ):
        raise VolatilityEvidenceContractErrorV2(
            "reason must be a bounded normalized identity"
        )


def _validate_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise VolatilityEvidenceContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _validate_nonnegative_int(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise VolatilityEvidenceContractErrorV2(
            f"{name} must be a nonnegative integer"
        )


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()
