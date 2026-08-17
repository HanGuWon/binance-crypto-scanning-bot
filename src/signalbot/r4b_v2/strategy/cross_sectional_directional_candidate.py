from __future__ import annotations

import hashlib
from dataclasses import InitVar, dataclass, field
from decimal import ROUND_FLOOR, Decimal, DecimalException, localcontext
from enum import StrEnum
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.cross_sectional_evidence import (
    CrossSectionalContextEvidenceV2,
    CrossSectionalContextStatusV2,
    CrossSectionalEvidenceContractErrorV2,
    canonical_cross_sectional_context_evidence_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import (
    EVIDENCE_STRENGTH_SCALE_V2,
)

CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_TARGET_EXCLUDED_CROSS_SECTION_DIRECTIONAL_V1_"
    "PRE_OUTCOME_NONPROMOTING"
)
CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_ROLE_V2: Final = (
    "PRE_OUTCOME_NON_PROMOTING_TARGET_EXCLUDED_DIRECTIONAL_CANDIDATE"
)

_SCHEMA_VERSION: Final = "r4b_cross_sectional_directional_candidate_v2"
_SOURCE_AUTHORITY_STATUS: Final = "TARGET_EXCLUDED_CONTEXT_M0_M1_M2_UNBOUND"
_INVALIDATION: Final = (
    "CANDIDATE_CANNOT_OPEN_CLOSE_FILTER_RANK_OR_PROMOTE_A_B_C_POSITIONS"
)
_EVENT_ID_DOMAIN: Final = b"R4B_CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_ID_V2\0"
_CANDIDATE_DOMAIN: Final = b"R4B_CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_V2\0"
_STRENGTH_SCALE: Final = Decimal(EVIDENCE_STRENGTH_SCALE_V2)
_FACTORY_TOKEN: Final = object()


class CrossSectionalDirectionalCandidateContractErrorV2(ValueError):
    """Raised when a target-excluded directional candidate is not exact."""


class CrossSectionalDirectionalCandidateStatusV2(StrEnum):
    READY = "READY"
    NOT_READY = "NOT_READY"


@dataclass(frozen=True, slots=True)
class _DerivedCandidateV2:
    status: CrossSectionalDirectionalCandidateStatusV2
    reasons: tuple[str, ...]
    m3_ex_target: Decimal | None
    shock_score: Decimal | None
    breadth_count: int | None
    breadth_denominator: int | None
    shock_magnitude: Decimal | None
    breadth_support: Decimal | None
    direction: int
    strength_micros: int
    signed_strength_micros: int


@dataclass(frozen=True, slots=True)
class CrossSectionalDirectionalCandidateV2:
    """Frozen pre-outcome mapping of one target-excluded context document."""

    source_context: CrossSectionalContextEvidenceV2 = field(repr=False)
    status: CrossSectionalDirectionalCandidateStatusV2
    reasons: tuple[str, ...]
    m3_ex_target: Decimal | None
    shock_score: Decimal | None
    breadth_count: int | None
    breadth_denominator: int | None
    shock_magnitude: Decimal | None
    breadth_support: Decimal | None
    direction: int
    strength_micros: int
    signed_strength_micros: int
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    candidate_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_RULE_VERSION_V2,
    )
    role: str = field(
        init=False,
        default=CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_ROLE_V2,
    )
    source_authority_status: str = field(
        init=False,
        default=_SOURCE_AUTHORITY_STATUS,
    )
    invalidation: str = field(init=False, default=_INVALIDATION)
    shadow_only: Literal[True] = field(init=False, default=True)
    pre_outcome_frozen: Literal[True] = field(init=False, default=True)
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
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    target_return_used: Literal[False] = field(init=False, default=False)
    primary_direction_used: Literal[False] = field(init=False, default=False)
    outcome_used: Literal[False] = field(init=False, default=False)
    data_through_ms: None = field(init=False, default=None)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise CrossSectionalDirectionalCandidateContractErrorV2(
                "cross-sectional directional candidates require their frozen factory"
            )
        _validate_candidate(self)
        object.__setattr__(self, "event_id", _event_id(self))
        object.__setattr__(
            self,
            "candidate_sha256",
            hashlib.sha256(
                _CANDIDATE_DOMAIN
                + canonical_json_line(
                    _candidate_document(self, include_candidate_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is CrossSectionalDirectionalCandidateStatusV2.READY

    @property
    def source_context_sha256(self) -> str:
        return self.source_context.evidence_sha256

    @property
    def target_symbol(self) -> str:
        return self.source_context.target_symbol

    @property
    def venue(self) -> VenueV2:
        return self.source_context.venue

    @property
    def ex_target_member_root_sha256(self) -> str:
        return self.source_context.ex_target_member_root_sha256

    @property
    def ex_target_slice_root_sha256(self) -> str:
        return self.source_context.ex_target_slice_root_sha256


def build_cross_sectional_directional_candidate_v2(
    source_context: CrossSectionalContextEvidenceV2,
) -> CrossSectionalDirectionalCandidateV2:
    """Map one canonical ex-target context without outcomes or promotion claims."""

    _validate_source_context(source_context)
    derived = _derive_candidate(source_context)
    return CrossSectionalDirectionalCandidateV2(
        source_context=source_context,
        status=derived.status,
        reasons=derived.reasons,
        m3_ex_target=derived.m3_ex_target,
        shock_score=derived.shock_score,
        breadth_count=derived.breadth_count,
        breadth_denominator=derived.breadth_denominator,
        shock_magnitude=derived.shock_magnitude,
        breadth_support=derived.breadth_support,
        direction=derived.direction,
        strength_micros=derived.strength_micros,
        signed_strength_micros=derived.signed_strength_micros,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_cross_sectional_directional_candidate_v2(
    value: CrossSectionalDirectionalCandidateV2,
) -> bytes:
    """Validate and serialize one frozen cross-sectional candidate."""

    if type(value) is not CrossSectionalDirectionalCandidateV2:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "value must be an exact CrossSectionalDirectionalCandidateV2"
        )
    _validate_candidate(value)
    if value.event_id != _event_id(value):
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "cross-sectional directional candidate event ID differs"
        )
    payload = canonical_json_line(
        _candidate_document(value, include_candidate_hash=False)
    )
    expected_hash = hashlib.sha256(_CANDIDATE_DOMAIN + payload).hexdigest()
    if value.candidate_sha256 != expected_hash:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "cross-sectional directional candidate hash differs"
        )
    return canonical_json_line(
        _candidate_document(value, include_candidate_hash=True)
    )


def _derive_candidate(
    source: CrossSectionalContextEvidenceV2,
) -> _DerivedCandidateV2:
    if source.status is not CrossSectionalContextStatusV2.READY:
        return _DerivedCandidateV2(
            status=CrossSectionalDirectionalCandidateStatusV2.NOT_READY,
            reasons=(
                "SOURCE_TARGET_EXCLUDED_CONTEXT_NOT_READY",
                f"SOURCE_STATUS_{source.status.value}",
                "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK",
                "PRE_OUTCOME_NON_PROMOTING_NO_EFFICACY_CLAIM",
            ),
            m3_ex_target=None,
            shock_score=None,
            breadth_count=None,
            breadth_denominator=None,
            shock_magnitude=None,
            breadth_support=None,
            direction=0,
            strength_micros=0,
            signed_strength_micros=0,
        )

    assert source.m3_ex_target is not None
    assert source.shock_score is not None
    assert source.breadth_count is not None
    assert source.breadth_denominator is not None
    try:
        with localcontext(protocol_decimal_context_v2()):
            shock_magnitude = source.shock_score / (
                Decimal(1) + source.shock_score
            )
            breadth_support = Decimal(source.breadth_count) / Decimal(
                source.breadth_denominator
            )
            strength = int(
                (
                    _STRENGTH_SCALE * shock_magnitude * breadth_support
                ).to_integral_value(rounding=ROUND_FLOOR)
            )
    except DecimalException as exc:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "READY source context cannot be mapped in the Decimal34 contract"
        ) from exc
    direction = _sign(source.m3_ex_target)
    return _DerivedCandidateV2(
        status=CrossSectionalDirectionalCandidateStatusV2.READY,
        reasons=(
            "TARGET_EXCLUDED_CONTEXT_DIRECTIONAL_CANDIDATE_READY",
            "DIRECTION_IS_M3_EX_TARGET_SIGN_INDEPENDENT_OF_STRENGTH_QUANTIZATION",
            "ROBUST_SHOCK_AND_SIGN_CONSISTENT_BREADTH_COMBINED_ONCE",
            "PRE_OUTCOME_NON_PROMOTING_NO_EFFICACY_CLAIM",
        ),
        m3_ex_target=source.m3_ex_target,
        shock_score=source.shock_score,
        breadth_count=source.breadth_count,
        breadth_denominator=source.breadth_denominator,
        shock_magnitude=shock_magnitude,
        breadth_support=breadth_support,
        direction=direction,
        strength_micros=strength,
        signed_strength_micros=direction * strength,
    )


def _validate_candidate(value: CrossSectionalDirectionalCandidateV2) -> None:
    _validate_source_context(value.source_context)
    if (
        value.schema_version != _SCHEMA_VERSION
        or value.rule_version
        != CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_RULE_VERSION_V2
        or value.role != CROSS_SECTIONAL_DIRECTIONAL_CANDIDATE_ROLE_V2
        or value.source_authority_status != _SOURCE_AUTHORITY_STATUS
        or value.invalidation != _INVALIDATION
        or value.shadow_only is not True
        or value.pre_outcome_frozen is not True
        or value.verified_raw_membership_m0_bound is not False
        or value.strict_source_parser_m1_bound is not False
        or value.causal_cursor_finality_m2_bound is not False
        or value.causal_inputs_complete is not False
        or value.producer_ready is not False
        or value.promoting is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.target_return_used is not False
        or value.primary_direction_used is not False
        or value.outcome_used is not False
        or value.data_through_ms is not None
    ):
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "candidate authority, use, or non-promotion flags differ"
        )
    expected = _derive_candidate(value.source_context)
    observed_fields = (
        value.status,
        value.reasons,
        value.m3_ex_target,
        value.shock_score,
        value.breadth_count,
        value.breadth_denominator,
        value.shock_magnitude,
        value.breadth_support,
        value.direction,
        value.strength_micros,
        value.signed_strength_micros,
    )
    expected_fields = (
        expected.status,
        expected.reasons,
        expected.m3_ex_target,
        expected.shock_score,
        expected.breadth_count,
        expected.breadth_denominator,
        expected.shock_magnitude,
        expected.breadth_support,
        expected.direction,
        expected.strength_micros,
        expected.signed_strength_micros,
    )
    if observed_fields != expected_fields:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "candidate fields contradict the bound source context"
        )
    _validate_reasons(value.reasons)
    if type(value.direction) is not int or value.direction not in (-1, 0, 1):
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "direction must be exactly -1, 0, or 1"
        )
    if (
        type(value.strength_micros) is not int
        or not 0 <= value.strength_micros <= EVIDENCE_STRENGTH_SCALE_V2
        or type(value.signed_strength_micros) is not int
        or value.signed_strength_micros != value.direction * value.strength_micros
    ):
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "candidate strength is outside its exact signed-magnitude contract"
        )
    if value.status is CrossSectionalDirectionalCandidateStatusV2.READY:
        if value.m3_ex_target is None or value.direction != _sign(
            value.m3_ex_target
        ):
            raise CrossSectionalDirectionalCandidateContractErrorV2(
                "READY direction must preserve the m3_ex_target sign"
            )
    elif value.status is not CrossSectionalDirectionalCandidateStatusV2.NOT_READY:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "candidate status is unsupported"
        )


def _validate_source_context(value: object) -> None:
    if type(value) is not CrossSectionalContextEvidenceV2:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "source_context must be an exact CrossSectionalContextEvidenceV2"
        )
    try:
        canonical_cross_sectional_context_evidence_v2(value)
    except CrossSectionalEvidenceContractErrorV2 as exc:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "source_context failed canonical validation"
        ) from exc


def _event_id(value: CrossSectionalDirectionalCandidateV2) -> str:
    source = value.source_context
    identity = {
        "bar_close_ms": source.bar_close_ms,
        "bar_open_ms": source.bar_open_ms,
        "decision_cutoff_ms": source.decision_cutoff_ms,
        "ex_target_member_root_sha256": source.ex_target_member_root_sha256,
        "ex_target_slice_root_sha256": source.ex_target_slice_root_sha256,
        "latest_source_event_ms": source.latest_source_event_ms,
        "latest_source_receipt_ms": source.latest_source_receipt_ms,
        "role": value.role,
        "rule_version": value.rule_version,
        "source_context_sha256": source.evidence_sha256,
        "target_symbol": source.target_symbol,
        "venue": source.venue.value,
    }
    return hashlib.sha256(
        _EVENT_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()


def _candidate_document(
    value: CrossSectionalDirectionalCandidateV2,
    *,
    include_candidate_hash: bool,
) -> dict[str, object]:
    source = value.source_context
    document: dict[str, object] = {
        "bar_close_ms": source.bar_close_ms,
        "bar_open_ms": source.bar_open_ms,
        "breadth_count": value.breadth_count,
        "breadth_denominator": value.breadth_denominator,
        "breadth_support": (
            None if value.breadth_support is None else str(value.breadth_support)
        ),
        "causal_cursor_finality_m2_bound": value.causal_cursor_finality_m2_bound,
        "causal_inputs_complete": value.causal_inputs_complete,
        "data_through_ms": value.data_through_ms,
        "decision_cutoff_ms": source.decision_cutoff_ms,
        "direction": value.direction,
        "event_id": value.event_id,
        "ex_target_member_root_sha256": source.ex_target_member_root_sha256,
        "ex_target_members": list(source.ex_target_members),
        "ex_target_slice_root_sha256": source.ex_target_slice_root_sha256,
        "invalidation": value.invalidation,
        "latest_source_event_ms": source.latest_source_event_ms,
        "latest_source_receipt_ms": source.latest_source_receipt_ms,
        "m3_ex_target": (
            None if value.m3_ex_target is None else str(value.m3_ex_target)
        ),
        "original_member_count": source.original_member_count,
        "original_panel_root_sha256": source.original_panel_root_sha256,
        "original_universe_root_sha256": source.original_universe_root_sha256,
        "outcome_used": value.outcome_used,
        "pre_outcome_frozen": value.pre_outcome_frozen,
        "primary_direction_used": value.primary_direction_used,
        "probability": value.probability,
        "probability_calibrated": value.probability_calibrated,
        "producer_ready": value.producer_ready,
        "promoting": value.promoting,
        "promoting_plan_sha256": source.promoting_plan_sha256,
        "reasons": list(value.reasons),
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "shadow_only": value.shadow_only,
        "shock_magnitude": (
            None if value.shock_magnitude is None else str(value.shock_magnitude)
        ),
        "shock_score": (
            None if value.shock_score is None else str(value.shock_score)
        ),
        "signed_strength_micros": value.signed_strength_micros,
        "source_authority_status": value.source_authority_status,
        "source_context_reasons": list(source.reasons),
        "source_context_rule_version": source.rule_version,
        "source_context_sha256": source.evidence_sha256,
        "source_context_status": source.status.value,
        "source_root_sha256": source.source_root_sha256,
        "status": value.status.value,
        "strength_micros": value.strength_micros,
        "strict_source_parser_m1_bound": value.strict_source_parser_m1_bound,
        "target_present": source.target_present,
        "target_return_used": value.target_return_used,
        "target_symbol": source.target_symbol,
        "venue": source.venue.value,
        "verified_raw_membership_m0_bound": (
            value.verified_raw_membership_m0_bound
        ),
    }
    if include_candidate_hash:
        document["candidate_sha256"] = value.candidate_sha256
    return document


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    if any(
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > 256
        for value in values
    ):
        raise CrossSectionalDirectionalCandidateContractErrorV2(
            "reasons must contain bounded normalized strings"
        )


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0
