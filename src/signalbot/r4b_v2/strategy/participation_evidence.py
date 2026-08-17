from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import ROUND_FLOOR, Decimal, DecimalException, localcontext
from enum import StrEnum
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
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBFlowOnlyBarEvidenceV2,
    canonical_family_b_flow_only_bar_evidence_v2,
)

PARTICIPATION_FLOW_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_PARTICIPATION_SIGNED_SHARE_8640_ACTIVITY_SHADOW_PROJECTION_NO_M0_M1_M2"
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_SLICE_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_FLOW_SLICE_V2\0"
_SOURCE_ROOT_DOMAIN: Final = b"R4B_PARTICIPATION_FLOW_SOURCE_ROOT_V2\0"
_EVIDENCE_DOMAIN: Final = b"R4B_PARTICIPATION_FLOW_EVIDENCE_V2\0"
_BAR_VALUE_DOMAIN: Final = b"R4B_PARTICIPATION_FLOW_BAR_VALUE_V2\0"
_CALCULATION_DOMAIN: Final = b"R4B_PARTICIPATION_FLOW_CALCULATION_V2\0"
_STRENGTH_SCALE: Final = Decimal(1_000_000)
_FACTORY_TOKEN: Final = object()
_BAR_VALUE_FACTORY_TOKEN: Final = object()
_CALCULATION_FACTORY_TOKEN: Final = object()
_SHADOW_AUTHORITY_REASONS: Final = (
    "VERIFIED_RAW_MEMBERSHIP_M0_ABSENT",
    "M0_ALONE_DOES_NOT_PROVE_CAUSAL_COMPLETENESS",
    "STRICT_SOURCE_PARSER_M1_ABSENT",
    "CAUSAL_CURSOR_FINALITY_M2_ABSENT",
)


class ParticipationFlowContractErrorV2(ValueError):
    """Raised when a participation-flow shadow slice is not causal or exact."""


class ParticipationFlowStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_WARMUP = "FEATURE_NOT_READY_WARMUP"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID_ARITHMETIC = "DATA_INVALID_ARITHMETIC"


@dataclass(frozen=True, slots=True)
class ParticipationFlowBarValueV2:
    """One source-neutral 5m flow value consumed by the frozen calculation."""

    bar_open_ms: int
    bar_close_ms: int
    signed_normal_notional: Decimal
    normal_notional: Decimal
    total_trade_notional: Decimal
    signed_share: Decimal | None
    _factory_token: InitVar[object | None] = None
    bar_value_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _BAR_VALUE_FACTORY_TOKEN:
            raise ParticipationFlowContractErrorV2(
                "participation bar values require their frozen factory"
            )
        _validate_flow_bar_value(self)
        object.__setattr__(
            self,
            "bar_value_sha256",
            hashlib.sha256(
                _BAR_VALUE_DOMAIN + canonical_json_line(_bar_value_document(self))
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.signed_share is not None


@dataclass(frozen=True, slots=True)
class ParticipationFlowCalculationV2:
    """Factory-sealed result of the frozen signed-share activity formula."""

    status: ParticipationFlowStatusV2
    reason: str
    prior_observation_count: int
    current_signed_share: Decimal | None
    current_total_trade_notional: Decimal | None
    prior_signed_share_location: Decimal | None
    prior_signed_share_mad: Decimal | None
    prior_signed_share_scale: Decimal | None
    prior_total_notional_median: Decimal | None
    scaled_signed_share_u: Decimal | None
    activity_support: Decimal | None
    direction: int
    strength_micros: int
    _factory_token: InitVar[object | None] = None
    calculation_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=PARTICIPATION_FLOW_RULE_VERSION_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CALCULATION_FACTORY_TOKEN:
            raise ParticipationFlowContractErrorV2(
                "participation calculations require their frozen factory"
            )
        _validate_calculation(self)
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
        return self.status is ParticipationFlowStatusV2.READY


@dataclass(frozen=True, slots=True)
class ParticipationFlowEvidenceV2:
    """Projection without verified M0 membership, M1 parsing, or M2 finality."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    normal_flow_capture_root_sha256: str
    normal_flow_nq_schema_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    current_projection_slice_sha256: str
    prior_flow_slice_sha256: str
    feature_slice_sha256: str
    source_lineage_root_sha256: str
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    status: ParticipationFlowStatusV2
    reasons: tuple[str, ...]
    prior_observation_count: int
    current_signed_share: Decimal | None
    current_total_trade_notional: Decimal | None
    prior_signed_share_location: Decimal | None
    prior_signed_share_mad: Decimal | None
    prior_signed_share_scale: Decimal | None
    prior_total_notional_median: Decimal | None
    scaled_signed_share_u: Decimal | None
    activity_support: Decimal | None
    direction: int
    strength_micros: int
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)
    shadow_only: bool = field(init=False, default=True)
    verified_raw_membership_m0_bound: bool = field(init=False, default=False)
    strict_source_parser_m1_bound: bool = field(init=False, default=False)
    causal_cursor_finality_m2_bound: bool = field(init=False, default=False)
    causal_inputs_complete: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ParticipationFlowContractErrorV2(
                "participation evidence must be created by its shadow factory"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise ParticipationFlowContractErrorV2(
                "participation evidence requires USD-M Futures provenance"
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
            (
                self.current_projection_slice_sha256,
                "current_projection_slice_sha256",
            ),
            (self.prior_flow_slice_sha256, "prior_flow_slice_sha256"),
            (self.feature_slice_sha256, "feature_slice_sha256"),
            (self.source_lineage_root_sha256, "source_lineage_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_bar(self.bar_open_ms, self.bar_close_ms, self.decision_cutoff_ms)
        for value, field_name in (
            (self.latest_source_event_ms, "latest_source_event_ms"),
            (self.latest_source_receipt_ms, "latest_source_receipt_ms"),
        ):
            _validate_nonnegative_int(value, field_name)
        if self.latest_source_event_ms <= self.bar_close_ms:
            raise ParticipationFlowContractErrorV2(
                "participation evidence requires a post-k.T completeness observation"
            )
        if self.latest_source_event_ms > self.latest_source_receipt_ms:
            raise ParticipationFlowContractErrorV2(
                "participation source event cannot follow its receipt"
            )
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise ParticipationFlowContractErrorV2(
                "source receipt after D cannot enter participation evidence"
            )
        if not isinstance(self.status, ParticipationFlowStatusV2):
            raise ParticipationFlowContractErrorV2("status must use ParticipationFlowStatusV2")
        if (
            type(self.prior_observation_count) is not int
            or not 0 <= self.prior_observation_count <= ROBUST_Z_PRIOR_WINDOW_V2
        ):
            raise ParticipationFlowContractErrorV2("prior_observation_count must be in [0, 8640]")
        if type(self.direction) is not int or self.direction not in (-1, 0, 1):
            raise ParticipationFlowContractErrorV2("direction must be exactly -1, 0, or 1")
        if type(self.strength_micros) is not int or not 0 <= self.strength_micros <= 1_000_000:
            raise ParticipationFlowContractErrorV2("strength_micros must be in [0, 1000000]")
        _validate_reasons(self.reasons)
        numeric = (
            self.current_signed_share,
            self.current_total_trade_notional,
            self.prior_signed_share_location,
            self.prior_signed_share_mad,
            self.prior_signed_share_scale,
            self.prior_total_notional_median,
            self.scaled_signed_share_u,
            self.activity_support,
        )
        if self.status is ParticipationFlowStatusV2.READY:
            if self.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2 or any(
                not _is_finite_decimal(value) for value in numeric
            ):
                raise ParticipationFlowContractErrorV2(
                    "READY participation evidence requires exact finite numeric evidence"
                )
            if (self.direction == 0) != (self.strength_micros == 0):
                raise ParticipationFlowContractErrorV2(
                    "READY direction and quantized strength must be zero together"
                )
            self._validate_ready_values()
        elif any(value is not None for value in numeric):
            raise ParticipationFlowContractErrorV2(
                "non-ready participation evidence cannot expose numeric fallbacks"
            )
        elif self.direction != 0 or self.strength_micros != 0:
            raise ParticipationFlowContractErrorV2(
                "non-ready participation evidence cannot expose direction or strength"
            )
        object.__setattr__(
            self,
            "evidence_sha256",
            hashlib.sha256(
                _EVIDENCE_DOMAIN + canonical_json_line(_evidence_document(self))
            ).hexdigest(),
        )

    def _validate_ready_values(self) -> None:
        assert self.current_signed_share is not None
        assert self.current_total_trade_notional is not None
        assert self.prior_signed_share_mad is not None
        assert self.prior_signed_share_scale is not None
        assert self.prior_total_notional_median is not None
        assert self.scaled_signed_share_u is not None
        assert self.activity_support is not None
        if not -1 <= self.current_signed_share <= 1:
            raise ParticipationFlowContractErrorV2("current_signed_share must be in [-1, 1]")
        if (
            self.current_total_trade_notional <= 0
            or self.prior_signed_share_mad <= 0
            or self.prior_signed_share_scale <= 0
            or self.prior_total_notional_median <= 0
            or not 0 < self.activity_support <= 1
        ):
            raise ParticipationFlowContractErrorV2(
                "READY participation scales, notionals, and support must be positive"
            )
        with localcontext(protocol_decimal_context_v2()):
            expected_u = self.current_signed_share / self.prior_signed_share_scale
            expected_activity = min(
                self.current_total_trade_notional / self.prior_total_notional_median,
                Decimal(1),
            )
            magnitude = abs(expected_u) / (Decimal(1) + abs(expected_u))
            expected_strength = int(
                (_STRENGTH_SCALE * magnitude * expected_activity).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
        if (
            self.scaled_signed_share_u != expected_u
            or self.activity_support != expected_activity
            or self.strength_micros != expected_strength
            or self.direction != (_sign(self.current_signed_share) if expected_strength > 0 else 0)
        ):
            raise ParticipationFlowContractErrorV2(
                "participation direction or frozen strength formula differs"
            )


def build_participation_flow_bar_value_v2(
    *,
    bar_open_ms: int,
    bar_close_ms: int,
    signed_normal_notional: Decimal,
    normal_notional: Decimal,
    total_trade_notional: Decimal,
    signed_share: Decimal | None,
) -> ParticipationFlowBarValueV2:
    """Seal one exact source-neutral flow bar for shared numeric evaluation."""

    return ParticipationFlowBarValueV2(
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        signed_normal_notional=signed_normal_notional,
        normal_notional=normal_notional,
        total_trade_notional=total_trade_notional,
        signed_share=signed_share,
        _factory_token=_BAR_VALUE_FACTORY_TOKEN,
    )


def calculate_participation_flow_v2(
    *,
    current_bar: ParticipationFlowBarValueV2,
    prior_bars: tuple[ParticipationFlowBarValueV2, ...],
) -> ParticipationFlowCalculationV2:
    """Run the frozen signed-share/MAD/activity formula without source claims."""

    if not isinstance(current_bar, ParticipationFlowBarValueV2):
        raise ParticipationFlowContractErrorV2("current_bar must be ParticipationFlowBarValueV2")
    canonical_participation_flow_bar_value_v2(current_bar)
    if type(prior_bars) is not tuple or any(
        not isinstance(value, ParticipationFlowBarValueV2) for value in prior_bars
    ):
        raise ParticipationFlowContractErrorV2(
            "prior_bars must be an immutable tuple of participation bar values"
        )
    if len(prior_bars) > ROBUST_Z_PRIOR_WINDOW_V2:
        raise ParticipationFlowContractErrorV2(
            "prior participation window cannot exceed 8,640 bars"
        )
    ordered_prior = tuple(sorted(prior_bars, key=lambda value: value.bar_open_ms))
    if len({value.bar_open_ms for value in ordered_prior}) != len(ordered_prior):
        raise ParticipationFlowContractErrorV2("prior participation bars must be unique")
    expected_start = current_bar.bar_open_ms - (len(ordered_prior) * FIVE_MINUTE_MS_V2)
    for index, value in enumerate(ordered_prior):
        canonical_participation_flow_bar_value_v2(value)
        if value.bar_open_ms != expected_start + index * FIVE_MINUTE_MS_V2:
            raise ParticipationFlowContractErrorV2(
                "prior participation bars must be exact contiguous slots"
            )

    if not current_bar.ready or any(not value.ready for value in ordered_prior):
        return _nonready_calculation(
            status=ParticipationFlowStatusV2.INCONCLUSIVE_DATA,
            reason="FLOW_ONLY_BAR_INCONCLUSIVE",
            prior_count=len(ordered_prior),
        )
    assert current_bar.signed_share is not None
    shares = tuple(value.signed_share for value in ordered_prior)
    assert all(value is not None for value in shares)
    exact_shares = tuple(value for value in shares if value is not None)
    robust = robust_z_v2(exact_shares, current_bar.signed_share)
    if robust.status is RobustZStatusV2.FEATURE_NOT_READY_WARMUP:
        return _nonready_calculation(
            status=ParticipationFlowStatusV2.FEATURE_NOT_READY_WARMUP,
            reason="EXACT_8640_PRIOR_FLOW_BARS_REQUIRED",
            prior_count=len(ordered_prior),
        )
    if robust.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE:
        return _nonready_calculation(
            status=ParticipationFlowStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
            reason="PRIOR_SIGNED_SHARE_MAD_ZERO",
            prior_count=len(ordered_prior),
        )
    if robust.status is not RobustZStatusV2.READY:
        return _nonready_calculation(
            status=ParticipationFlowStatusV2.DATA_INVALID_ARITHMETIC,
            reason="SIGNED_SHARE_ROBUST_SCALE_INVALID",
            prior_count=len(ordered_prior),
        )

    assert robust.location is not None
    assert robust.mad is not None
    assert robust.scale is not None
    prior_totals = tuple(value.total_trade_notional for value in ordered_prior)
    try:
        with localcontext(protocol_decimal_context_v2()):
            prior_total_median = _median_decimal(prior_totals)
            if prior_total_median <= 0:
                raise ParticipationFlowContractErrorV2(
                    "prior total-notional median must be positive"
                )
            scaled_u = current_bar.signed_share / robust.scale
            activity_support = min(
                current_bar.total_trade_notional / prior_total_median,
                Decimal(1),
            )
            magnitude = abs(scaled_u) / (Decimal(1) + abs(scaled_u))
            strength = int(
                (_STRENGTH_SCALE * magnitude * activity_support).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
    except DecimalException:
        return _nonready_calculation(
            status=ParticipationFlowStatusV2.DATA_INVALID_ARITHMETIC,
            reason="PARTICIPATION_DECIMAL_ARITHMETIC_INVALID",
            prior_count=len(ordered_prior),
        )
    return ParticipationFlowCalculationV2(
        status=ParticipationFlowStatusV2.READY,
        reason="SIGNED_SHARE_MAD_ACTIVITY_SHADOW_READY",
        prior_observation_count=len(ordered_prior),
        current_signed_share=current_bar.signed_share,
        current_total_trade_notional=current_bar.total_trade_notional,
        prior_signed_share_location=robust.location,
        prior_signed_share_mad=robust.mad,
        prior_signed_share_scale=robust.scale,
        prior_total_notional_median=prior_total_median,
        scaled_signed_share_u=scaled_u,
        activity_support=activity_support,
        direction=_sign(current_bar.signed_share) if strength > 0 else 0,
        strength_micros=strength,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def canonical_participation_flow_bar_value_v2(
    value: ParticipationFlowBarValueV2,
) -> bytes:
    """Serialize and validate one shared source-neutral flow bar value."""

    if not isinstance(value, ParticipationFlowBarValueV2):
        raise ParticipationFlowContractErrorV2("value must be ParticipationFlowBarValueV2")
    _validate_flow_bar_value(value)
    payload = canonical_json_line(_bar_value_document(value))
    expected = hashlib.sha256(_BAR_VALUE_DOMAIN + payload).hexdigest()
    if value.bar_value_sha256 != expected:
        raise ParticipationFlowContractErrorV2(
            "participation bar value differs from canonical content"
        )
    return payload


def canonical_participation_flow_calculation_v2(
    value: ParticipationFlowCalculationV2,
) -> bytes:
    """Serialize and validate one shared frozen participation calculation."""

    if not isinstance(value, ParticipationFlowCalculationV2):
        raise ParticipationFlowContractErrorV2("value must be ParticipationFlowCalculationV2")
    _validate_calculation(value)
    payload = canonical_json_line(_calculation_document(value, include_calculation_hash=False))
    expected = hashlib.sha256(_CALCULATION_DOMAIN + payload).hexdigest()
    if value.calculation_sha256 != expected:
        raise ParticipationFlowContractErrorV2(
            "participation calculation differs from canonical content"
        )
    return canonical_json_line(_calculation_document(value, include_calculation_hash=True))


def build_participation_flow_evidence_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    current_bar: FamilyBFlowOnlyBarEvidenceV2,
    prior_bars: tuple[FamilyBFlowOnlyBarEvidenceV2, ...],
) -> ParticipationFlowEvidenceV2:
    """Build a projection-only shadow rule without primary-side/outcome inputs."""

    _validate_scope(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    if not isinstance(current_bar, FamilyBFlowOnlyBarEvidenceV2):
        raise ParticipationFlowContractErrorV2("current_bar must be FamilyBFlowOnlyBarEvidenceV2")
    canonical_family_b_flow_only_bar_evidence_v2(current_bar)
    if type(prior_bars) is not tuple or any(
        not isinstance(value, FamilyBFlowOnlyBarEvidenceV2) for value in prior_bars
    ):
        raise ParticipationFlowContractErrorV2(
            "prior_bars must be an immutable tuple of flow-only bar evidence"
        )
    if len(prior_bars) > ROBUST_Z_PRIOR_WINDOW_V2:
        raise ParticipationFlowContractErrorV2("prior flow window cannot exceed 8,640 bars")
    ordered_prior = tuple(sorted(prior_bars, key=lambda value: value.bar_open_ms))
    if len({value.bar_open_ms for value in ordered_prior}) != len(ordered_prior):
        raise ParticipationFlowContractErrorV2("prior flow bars must be unique")

    expected_identity = (
        attempt_id,
        symbol,
        venue,
        promoting_plan_sha256,
        current_bar.normal_flow_capture_root_sha256,
        current_bar.normal_flow_nq_schema_sha256,
    )
    _validate_bar_identity(current_bar, expected_identity)
    if (
        current_bar.bar_open_ms,
        current_bar.bar_close_ms,
        current_bar.decision_cutoff_ms,
    ) != (bar_open_ms, bar_close_ms, decision_cutoff_ms):
        raise ParticipationFlowContractErrorV2(
            "current flow-only bar differs from the participation decision slot"
        )
    expected_start = bar_open_ms - len(ordered_prior) * FIVE_MINUTE_MS_V2
    for index, value in enumerate(ordered_prior):
        canonical_family_b_flow_only_bar_evidence_v2(value)
        _validate_bar_identity(value, expected_identity)
        expected_open = expected_start + index * FIVE_MINUTE_MS_V2
        if value.bar_open_ms != expected_open:
            raise ParticipationFlowContractErrorV2(
                "prior flow bars must be exact contiguous t-W through t-1"
            )
        if value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
            raise ParticipationFlowContractErrorV2("prior flow bar has an invalid exact 5m close")

    current_projection = _flow_bar_projection_document(current_bar)
    prior_rows = [_flow_bar_projection_document(value) for value in ordered_prior]
    current_slice_root = _slice_root("CURRENT_FLOW_ONLY_BAR", [current_projection])
    prior_slice_root = _slice_root("PRIOR_FLOW_ONLY_BARS", prior_rows)
    feature_slice_root = _slice_root(
        "PARTICIPATION_FLOW_FEATURE",
        [
            {
                "current_projection_slice_sha256": current_slice_root,
                "prior_flow_slice_sha256": prior_slice_root,
            }
        ],
    )
    source_root = _source_root(
        current_bar=current_bar,
        ordered_prior=ordered_prior,
        current_projection_slice_sha256=current_slice_root,
        prior_flow_slice_sha256=prior_slice_root,
    )
    latest_event = max(
        [
            current_bar.latest_source_event_ms,
            *(value.latest_source_event_ms for value in ordered_prior),
        ]
    )
    latest_receipt = max(
        [
            current_bar.latest_source_receipt_ms,
            *(value.latest_source_receipt_ms for value in ordered_prior),
        ]
    )
    common = {
        "attempt_id": attempt_id,
        "symbol": symbol,
        "venue": venue,
        "promoting_plan_sha256": promoting_plan_sha256,
        "normal_flow_capture_root_sha256": (current_bar.normal_flow_capture_root_sha256),
        "normal_flow_nq_schema_sha256": current_bar.normal_flow_nq_schema_sha256,
        "bar_open_ms": bar_open_ms,
        "bar_close_ms": bar_close_ms,
        "decision_cutoff_ms": decision_cutoff_ms,
        "current_projection_slice_sha256": current_slice_root,
        "prior_flow_slice_sha256": prior_slice_root,
        "feature_slice_sha256": feature_slice_root,
        "source_lineage_root_sha256": source_root,
        "latest_source_event_ms": latest_event,
        "latest_source_receipt_ms": latest_receipt,
        "prior_observation_count": len(ordered_prior),
    }

    current_value = build_participation_flow_bar_value_v2(
        bar_open_ms=current_bar.bar_open_ms,
        bar_close_ms=current_bar.bar_close_ms,
        signed_normal_notional=current_bar.signed_normal_notional,
        normal_notional=current_bar.normal_notional,
        total_trade_notional=current_bar.total_trade_notional,
        signed_share=current_bar.signed_share,
    )
    prior_values = tuple(
        build_participation_flow_bar_value_v2(
            bar_open_ms=value.bar_open_ms,
            bar_close_ms=value.bar_close_ms,
            signed_normal_notional=value.signed_normal_notional,
            normal_notional=value.normal_notional,
            total_trade_notional=value.total_trade_notional,
            signed_share=value.signed_share,
        )
        for value in ordered_prior
    )
    calculation = calculate_participation_flow_v2(
        current_bar=current_value,
        prior_bars=prior_values,
    )
    if not calculation.ready:
        return _nonready(
            **common,
            status=calculation.status,
            reasons=(calculation.reason, *_SHADOW_AUTHORITY_REASONS),
        )
    return ParticipationFlowEvidenceV2(
        **common,
        status=ParticipationFlowStatusV2.READY,
        reasons=(calculation.reason, *_SHADOW_AUTHORITY_REASONS),
        current_signed_share=calculation.current_signed_share,
        current_total_trade_notional=calculation.current_total_trade_notional,
        prior_signed_share_location=calculation.prior_signed_share_location,
        prior_signed_share_mad=calculation.prior_signed_share_mad,
        prior_signed_share_scale=calculation.prior_signed_share_scale,
        prior_total_notional_median=calculation.prior_total_notional_median,
        scaled_signed_share_u=calculation.scaled_signed_share_u,
        activity_support=calculation.activity_support,
        direction=calculation.direction,
        strength_micros=calculation.strength_micros,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_participation_flow_evidence_v2(
    evidence: ParticipationFlowEvidenceV2,
) -> bytes:
    if not isinstance(evidence, ParticipationFlowEvidenceV2):
        raise ParticipationFlowContractErrorV2("evidence must be ParticipationFlowEvidenceV2")
    payload = canonical_json_line(_evidence_document(evidence))
    expected = hashlib.sha256(_EVIDENCE_DOMAIN + payload).hexdigest()
    if evidence.evidence_sha256 != expected:
        raise ParticipationFlowContractErrorV2(
            "participation evidence hash differs from canonical content"
        )
    return payload


def _nonready(
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
    current_projection_slice_sha256: str,
    prior_flow_slice_sha256: str,
    feature_slice_sha256: str,
    source_lineage_root_sha256: str,
    latest_source_event_ms: int,
    latest_source_receipt_ms: int,
    prior_observation_count: int,
    status: ParticipationFlowStatusV2,
    reasons: tuple[str, ...],
) -> ParticipationFlowEvidenceV2:
    return ParticipationFlowEvidenceV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        normal_flow_capture_root_sha256=normal_flow_capture_root_sha256,
        normal_flow_nq_schema_sha256=normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        current_projection_slice_sha256=current_projection_slice_sha256,
        prior_flow_slice_sha256=prior_flow_slice_sha256,
        feature_slice_sha256=feature_slice_sha256,
        source_lineage_root_sha256=source_lineage_root_sha256,
        latest_source_event_ms=latest_source_event_ms,
        latest_source_receipt_ms=latest_source_receipt_ms,
        status=status,
        reasons=reasons,
        prior_observation_count=prior_observation_count,
        current_signed_share=None,
        current_total_trade_notional=None,
        prior_signed_share_location=None,
        prior_signed_share_mad=None,
        prior_signed_share_scale=None,
        prior_total_notional_median=None,
        scaled_signed_share_u=None,
        activity_support=None,
        direction=0,
        strength_micros=0,
        _factory_token=_FACTORY_TOKEN,
    )


def _flow_bar_projection_document(
    value: FamilyBFlowOnlyBarEvidenceV2,
) -> dict[str, object]:
    """Return only slot/readiness and derived economic values.

    Raw-slice and evidence identities belong exclusively to the separate source
    lineage root.  Keeping them out of this document makes economic alias
    detection independent of which valid source lineage produced the values.
    """

    canonical_family_b_flow_only_bar_evidence_v2(value)
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "flow_imbalance": (None if value.flow_imbalance is None else str(value.flow_imbalance)),
        "normal_notional": str(value.normal_notional),
        "readiness": value.readiness.value,
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": None if value.signed_share is None else str(value.signed_share),
        "total_trade_notional": str(value.total_trade_notional),
    }


def _slice_root(label: str, rows: list[dict[str, object]]) -> str:
    return hashlib.sha256(
        _SLICE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "label": label,
                "rows": rows,
                "schema_version": "r4b_participation_flow_slice_v2",
            }
        )
    ).hexdigest()


def _source_root(
    *,
    current_bar: FamilyBFlowOnlyBarEvidenceV2,
    ordered_prior: tuple[FamilyBFlowOnlyBarEvidenceV2, ...],
    current_projection_slice_sha256: str,
    prior_flow_slice_sha256: str,
) -> str:
    return hashlib.sha256(
        _SOURCE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "current_projection_slice_sha256": (current_projection_slice_sha256),
                "current_source": _flow_bar_lineage_document(current_bar),
                "normal_flow_capture_root_sha256": (current_bar.normal_flow_capture_root_sha256),
                "normal_flow_nq_schema_sha256": (current_bar.normal_flow_nq_schema_sha256),
                "prior_flow_slice_sha256": prior_flow_slice_sha256,
                "prior_sources": [_flow_bar_lineage_document(value) for value in ordered_prior],
                "schema_version": "r4b_participation_flow_source_root_v2",
            }
        )
    ).hexdigest()


def _flow_bar_lineage_document(
    value: FamilyBFlowOnlyBarEvidenceV2,
) -> dict[str, object]:
    return {
        "bar_open_ms": value.bar_open_ms,
        "flow_bar_evidence_sha256": value.flow_bar_evidence_sha256,
        "flow_source_root_sha256": value.flow_source_root_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "normal_flow_slice_sha256": value.normal_flow_slice_sha256,
    }


def _evidence_document(value: ParticipationFlowEvidenceV2) -> dict[str, object]:
    return {
        "activity_support": _decimal_or_none(value.activity_support),
        "attempt_id": value.attempt_id,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "current_projection_slice_sha256": (value.current_projection_slice_sha256),
        "current_signed_share": _decimal_or_none(value.current_signed_share),
        "current_total_trade_notional": _decimal_or_none(value.current_total_trade_notional),
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "direction": value.direction,
        "feature_slice_sha256": value.feature_slice_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "normal_flow_capture_root_sha256": (value.normal_flow_capture_root_sha256),
        "normal_flow_nq_schema_sha256": value.normal_flow_nq_schema_sha256,
        "prior_flow_slice_sha256": value.prior_flow_slice_sha256,
        "prior_observation_count": value.prior_observation_count,
        "prior_signed_share_location": _decimal_or_none(value.prior_signed_share_location),
        "prior_signed_share_mad": _decimal_or_none(value.prior_signed_share_mad),
        "prior_signed_share_scale": _decimal_or_none(value.prior_signed_share_scale),
        "prior_total_notional_median": _decimal_or_none(value.prior_total_notional_median),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "causal_cursor_finality_m2_bound": (value.causal_cursor_finality_m2_bound),
        "causal_inputs_complete": value.causal_inputs_complete,
        "reasons": list(value.reasons),
        "rule_version": PARTICIPATION_FLOW_RULE_VERSION_V2,
        "scaled_signed_share_u": _decimal_or_none(value.scaled_signed_share_u),
        "schema_version": "r4b_participation_flow_evidence_v2",
        "shadow_only": value.shadow_only,
        "source_lineage_root_sha256": value.source_lineage_root_sha256,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "strict_source_parser_m1_bound": value.strict_source_parser_m1_bound,
        "symbol": value.symbol,
        "venue": value.venue.value,
        "verified_raw_membership_m0_bound": (value.verified_raw_membership_m0_bound),
    }


def _validate_bar_identity(
    value: FamilyBFlowOnlyBarEvidenceV2,
    expected: tuple[str, str, VenueV2, str, str, str],
) -> None:
    observed = (
        value.attempt_id,
        value.symbol,
        value.venue,
        value.promoting_plan_sha256,
        value.normal_flow_capture_root_sha256,
        value.normal_flow_nq_schema_sha256,
    )
    if observed != expected:
        raise ParticipationFlowContractErrorV2(
            "flow-only bar differs from participation identity or lineage"
        )


def _validate_scope(
    *,
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
        raise ParticipationFlowContractErrorV2("participation evidence accepts USD-M Futures only")
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    _validate_bar(bar_open_ms, bar_close_ms, decision_cutoff_ms)


def _validate_bar(bar_open_ms: int, bar_close_ms: int, decision_cutoff_ms: int) -> None:
    try:
        validate_decision_bar_v2(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    except DecisionClockContractErrorV2 as exc:
        raise ParticipationFlowContractErrorV2(str(exc)) from exc


def _nonready_calculation(
    *,
    status: ParticipationFlowStatusV2,
    reason: str,
    prior_count: int,
) -> ParticipationFlowCalculationV2:
    return ParticipationFlowCalculationV2(
        status=status,
        reason=reason,
        prior_observation_count=prior_count,
        current_signed_share=None,
        current_total_trade_notional=None,
        prior_signed_share_location=None,
        prior_signed_share_mad=None,
        prior_signed_share_scale=None,
        prior_total_notional_median=None,
        scaled_signed_share_u=None,
        activity_support=None,
        direction=0,
        strength_micros=0,
        _factory_token=_CALCULATION_FACTORY_TOKEN,
    )


def _validate_flow_bar_value(value: ParticipationFlowBarValueV2) -> None:
    if (
        type(value.bar_open_ms) is not int
        or value.bar_open_ms < 0
        or value.bar_open_ms % FIVE_MINUTE_MS_V2 != 0
        or type(value.bar_close_ms) is not int
        or value.bar_close_ms != value.bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    ):
        raise ParticipationFlowContractErrorV2(
            "participation bar value must bind one exact aligned 5m slot"
        )
    for decimal_value, name in (
        (value.signed_normal_notional, "signed_normal_notional"),
        (value.normal_notional, "normal_notional"),
        (value.total_trade_notional, "total_trade_notional"),
    ):
        if not _is_finite_decimal(decimal_value):
            raise ParticipationFlowContractErrorV2(f"{name} must be finite Decimal")
    if (
        value.normal_notional < 0
        or value.total_trade_notional < 0
        or abs(value.signed_normal_notional) > value.normal_notional
        or value.normal_notional > value.total_trade_notional
    ):
        raise ParticipationFlowContractErrorV2(
            "participation bar notionals violate signed <= normal <= total"
        )
    ready = value.normal_notional > 0 and value.total_trade_notional > 0
    if ready:
        if not _is_finite_decimal(value.signed_share):
            raise ParticipationFlowContractErrorV2(
                "ready participation bar requires finite signed_share"
            )
        with localcontext(protocol_decimal_context_v2()):
            expected_share = value.signed_normal_notional / value.total_trade_notional
        if value.signed_share != expected_share:
            raise ParticipationFlowContractErrorV2(
                "participation signed_share contradicts its notionals"
            )
    elif value.signed_share is not None:
        raise ParticipationFlowContractErrorV2(
            "inconclusive participation bar cannot expose signed_share"
        )


def _validate_calculation(value: ParticipationFlowCalculationV2) -> None:
    if not isinstance(value.status, ParticipationFlowStatusV2):
        raise ParticipationFlowContractErrorV2(
            "calculation status must use ParticipationFlowStatusV2"
        )
    _validate_reasons((value.reason,))
    if (
        type(value.prior_observation_count) is not int
        or not 0 <= value.prior_observation_count <= ROBUST_Z_PRIOR_WINDOW_V2
    ):
        raise ParticipationFlowContractErrorV2("calculation prior count must be in [0, 8640]")
    if type(value.direction) is not int or value.direction not in (-1, 0, 1):
        raise ParticipationFlowContractErrorV2("calculation direction must be -1, 0, or 1")
    if type(value.strength_micros) is not int or not 0 <= value.strength_micros <= 1_000_000:
        raise ParticipationFlowContractErrorV2("calculation strength must be in [0, 1000000]")
    numeric = _calculation_numeric_values(value)
    if value.status is ParticipationFlowStatusV2.READY:
        if value.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2 or any(
            not _is_finite_decimal(item) for item in numeric
        ):
            raise ParticipationFlowContractErrorV2(
                "READY calculation requires exact finite numeric evidence"
            )
        _validate_ready_calculation(value)
    elif any(item is not None for item in numeric):
        raise ParticipationFlowContractErrorV2("non-ready calculation cannot expose numeric values")
    elif value.direction != 0 or value.strength_micros != 0:
        raise ParticipationFlowContractErrorV2("non-ready calculation must remain neutral")


def _validate_ready_calculation(value: ParticipationFlowCalculationV2) -> None:
    assert value.current_signed_share is not None
    assert value.current_total_trade_notional is not None
    assert value.prior_signed_share_mad is not None
    assert value.prior_signed_share_scale is not None
    assert value.prior_total_notional_median is not None
    assert value.scaled_signed_share_u is not None
    assert value.activity_support is not None
    if (
        value.reason != "SIGNED_SHARE_MAD_ACTIVITY_SHADOW_READY"
        or not -1 <= value.current_signed_share <= 1
        or value.current_total_trade_notional <= 0
        or value.prior_signed_share_mad <= 0
        or value.prior_signed_share_scale <= 0
        or value.prior_total_notional_median <= 0
        or not 0 < value.activity_support <= 1
    ):
        raise ParticipationFlowContractErrorV2(
            "READY calculation scalar bounds differ from the frozen rule"
        )
    with localcontext(protocol_decimal_context_v2()):
        expected_u = value.current_signed_share / value.prior_signed_share_scale
        expected_activity = min(
            value.current_total_trade_notional / value.prior_total_notional_median,
            Decimal(1),
        )
        magnitude = abs(expected_u) / (Decimal(1) + abs(expected_u))
        expected_strength = int(
            (_STRENGTH_SCALE * magnitude * expected_activity).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
    expected_direction = _sign(value.current_signed_share) if expected_strength > 0 else 0
    if (
        value.scaled_signed_share_u != expected_u
        or value.activity_support != expected_activity
        or value.strength_micros != expected_strength
        or value.direction != expected_direction
    ):
        raise ParticipationFlowContractErrorV2(
            "participation calculation contradicts the frozen formula"
        )


def _calculation_numeric_values(
    value: ParticipationFlowCalculationV2,
) -> tuple[Decimal | None, ...]:
    return (
        value.current_signed_share,
        value.current_total_trade_notional,
        value.prior_signed_share_location,
        value.prior_signed_share_mad,
        value.prior_signed_share_scale,
        value.prior_total_notional_median,
        value.scaled_signed_share_u,
        value.activity_support,
    )


def _bar_value_document(value: ParticipationFlowBarValueV2) -> dict[str, object]:
    return {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "normal_notional": str(value.normal_notional),
        "schema_version": "r4b_participation_flow_bar_value_v2",
        "signed_normal_notional": str(value.signed_normal_notional),
        "signed_share": _decimal_or_none(value.signed_share),
        "total_trade_notional": str(value.total_trade_notional),
    }


def _calculation_document(
    value: ParticipationFlowCalculationV2,
    *,
    include_calculation_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "activity_support": _decimal_or_none(value.activity_support),
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
        "schema_version": "r4b_participation_flow_calculation_v2",
        "status": value.status.value,
        "strength_micros": value.strength_micros,
    }
    if include_calculation_hash:
        document["calculation_sha256"] = value.calculation_sha256
    return document


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise ParticipationFlowContractErrorV2("median requires a non-empty tuple")
    if any(not _is_finite_decimal(value) for value in values):
        raise ParticipationFlowContractErrorV2("median inputs must be finite Decimal")
    ordered = tuple(sorted(values))
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _validate_reasons(values: tuple[str, ...]) -> None:
    if (
        type(values) is not tuple
        or not values
        or len(values) > 8
        or any(
            not isinstance(value, str) or not value or value.strip() != value or len(value) > 128
            for value in values
        )
    ):
        raise ParticipationFlowContractErrorV2("reasons must be a non-empty bounded tuple")


def _validate_identity(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise ParticipationFlowContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_symbol(value: object) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise ParticipationFlowContractErrorV2("symbol must be a normalized USDT symbol")


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ParticipationFlowContractErrorV2(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ParticipationFlowContractErrorV2(f"{field_name} must be a nonnegative integer")


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()
