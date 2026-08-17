from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException, localcontext
from enum import StrEnum
from typing import Final, TypedDict

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_MINIMUM_MEMBERS_V2,
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FAMILY_C_PRIOR_WINDOW_V2,
    FamilyCCandlePanelV2,
    canonical_family_c_candle_panel_v2,
)

CROSS_SECTIONAL_CONTEXT_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.2.0_TARGET_EXCLUDED_CROSS_SECTION_V1_SHADOW"
)
CROSS_SECTIONAL_CONTEXT_MIN_MEMBERS_V2: Final = 19

_MAD_SCALE: Final = Decimal("1.4826")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_MEMBER_ROOT_DOMAIN: Final = b"R4B_TARGET_EXCLUDED_MEMBER_ROOT_V2\0"
_SLICE_ROOT_DOMAIN: Final = b"R4B_TARGET_EXCLUDED_CONTEXT_SLICE_V2\0"
_EVIDENCE_DOMAIN: Final = b"R4B_TARGET_EXCLUDED_CONTEXT_EVIDENCE_V2\0"
_FACTORY_TOKEN: Final = object()


class _CommonFieldsV2(TypedDict):
    target_symbol: str
    target_present: bool
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    original_universe_root_sha256: str
    original_panel_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    original_member_count: int
    ex_target_members: tuple[str, ...]
    ex_target_member_root_sha256: str
    ex_target_slice_root_sha256: str


class CrossSectionalEvidenceContractErrorV2(ValueError):
    """Raised when target-excluded context is not causal and self-consistent."""


class CrossSectionalContextStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_MEMBER_COUNT = "FEATURE_NOT_READY_MEMBER_COUNT"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    DATA_INVALID_ARITHMETIC = "DATA_INVALID_ARITHMETIC"


@dataclass(frozen=True, slots=True)
class CrossSectionalContextEvidenceV2:
    """Factory-sealed target-excluded market context from one exact C panel."""

    target_symbol: str
    target_present: bool
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    original_universe_root_sha256: str
    original_panel_root_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    original_member_count: int
    ex_target_members: tuple[str, ...]
    ex_target_member_root_sha256: str
    ex_target_slice_root_sha256: str
    prior_observation_count: int
    status: CrossSectionalContextStatusV2
    reasons: tuple[str, ...]
    m3_ex_target: Decimal | None
    shock_scale: Decimal | None
    shock_score: Decimal | None
    breadth_count: int | None
    breadth_denominator: int | None
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=CROSS_SECTIONAL_CONTEXT_RULE_VERSION_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise CrossSectionalEvidenceContractErrorV2(
                "cross-sectional context must be created by its causal factory"
            )
        _validate_symbol(self.target_symbol)
        if type(self.target_present) is not bool:
            raise CrossSectionalEvidenceContractErrorV2(
                "target_present must be boolean"
            )
        if self.venue is not VenueV2.USDM_FUTURES:
            raise CrossSectionalEvidenceContractErrorV2(
                "cross-sectional context requires USD-M Futures"
            )
        for value, name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.source_root_sha256, "source_root_sha256"),
            (self.original_universe_root_sha256, "original_universe_root_sha256"),
            (self.original_panel_root_sha256, "original_panel_root_sha256"),
            (self.ex_target_member_root_sha256, "ex_target_member_root_sha256"),
            (self.ex_target_slice_root_sha256, "ex_target_slice_root_sha256"),
        ):
            _validate_sha256(value, name)
        for value, name in (
            (self.bar_open_ms, "bar_open_ms"),
            (self.bar_close_ms, "bar_close_ms"),
            (self.decision_cutoff_ms, "decision_cutoff_ms"),
            (self.latest_source_event_ms, "latest_source_event_ms"),
            (self.latest_source_receipt_ms, "latest_source_receipt_ms"),
            (self.original_member_count, "original_member_count"),
            (self.prior_observation_count, "prior_observation_count"),
        ):
            _validate_nonnegative_int(value, name)
        if self.latest_source_event_ms > self.bar_close_ms:
            raise CrossSectionalEvidenceContractErrorV2(
                "future source event cannot enter cross-sectional context"
            )
        if self.latest_source_receipt_ms > self.decision_cutoff_ms:
            raise CrossSectionalEvidenceContractErrorV2(
                "source receipt after D cannot enter cross-sectional context"
            )
        _validate_member_set(self.ex_target_members)
        if self.target_symbol in self.ex_target_members:
            raise CrossSectionalEvidenceContractErrorV2(
                "target exclusion flag contradicts the ex-target member set"
            )
        expected_count = len(self.ex_target_members) + int(self.target_present)
        if self.original_member_count != expected_count:
            raise CrossSectionalEvidenceContractErrorV2(
                "original member count contradicts deterministic target exclusion"
            )
        if self.ex_target_member_root_sha256 != _member_root(
            self.target_symbol,
            self.target_present,
            self.ex_target_members,
        ):
            raise CrossSectionalEvidenceContractErrorV2(
                "ex-target member root differs from canonical membership"
            )
        if not isinstance(self.status, CrossSectionalContextStatusV2):
            raise CrossSectionalEvidenceContractErrorV2(
                "status must be CrossSectionalContextStatusV2"
            )
        _validate_reasons(self.reasons)
        scalars = (
            self.m3_ex_target,
            self.shock_scale,
            self.shock_score,
            self.breadth_count,
            self.breadth_denominator,
        )
        if self.status is not CrossSectionalContextStatusV2.READY:
            if any(value is not None for value in scalars):
                raise CrossSectionalEvidenceContractErrorV2(
                    "non-ready context cannot expose partial numeric evidence"
                )
        else:
            if self.prior_observation_count != FAMILY_C_PRIOR_WINDOW_V2:
                raise CrossSectionalEvidenceContractErrorV2(
                    "READY context requires exactly 8,640 prior observations"
                )
            if not _is_finite_decimal(self.m3_ex_target):
                raise CrossSectionalEvidenceContractErrorV2(
                    "READY context requires finite m3_ex_target"
                )
            if not _is_positive_finite(self.shock_scale):
                raise CrossSectionalEvidenceContractErrorV2(
                    "READY context requires positive shock_scale"
                )
            if not _is_nonnegative_finite(self.shock_score):
                raise CrossSectionalEvidenceContractErrorV2(
                    "READY context requires nonnegative shock_score"
                )
            assert self.m3_ex_target is not None
            assert self.shock_scale is not None
            assert self.shock_score is not None
            with localcontext(protocol_decimal_context_v2()):
                if self.shock_score != abs(self.m3_ex_target) / self.shock_scale:
                    raise CrossSectionalEvidenceContractErrorV2(
                        "shock_score differs from abs(m3_ex_target) / shock_scale"
                    )
            if (
                type(self.breadth_count) is not int
                or type(self.breadth_denominator) is not int
                or self.breadth_denominator != len(self.ex_target_members)
                or not 0 <= self.breadth_count <= self.breadth_denominator
            ):
                raise CrossSectionalEvidenceContractErrorV2(
                    "breadth fields contradict the target-excluded member set"
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
        return self.status is CrossSectionalContextStatusV2.READY


def build_target_excluded_cross_section_evidence_v2(
    panel: FamilyCCandlePanelV2,
    *,
    target_symbol: str,
) -> CrossSectionalContextEvidenceV2:
    """Derive market context only after deterministic target exclusion."""

    if not isinstance(panel, FamilyCCandlePanelV2):
        raise CrossSectionalEvidenceContractErrorV2(
            "panel must be FamilyCCandlePanelV2"
        )
    _validate_symbol(target_symbol)
    canonical_family_c_candle_panel_v2(panel)
    target_present = target_symbol in panel.universe.members
    ex_target_members = tuple(
        member for member in panel.universe.members if member != target_symbol
    )
    member_root = _member_root(
        target_symbol,
        target_present,
        ex_target_members,
    )
    slice_root = _slice_root(panel, target_symbol, target_present, ex_target_members)
    common = _common_fields(
        panel,
        target_symbol=target_symbol,
        target_present=target_present,
        ex_target_members=ex_target_members,
        ex_target_member_root_sha256=member_root,
        ex_target_slice_root_sha256=slice_root,
    )
    if (
        len(panel.universe.members) < FAMILY_C_MINIMUM_MEMBERS_V2
        or len(ex_target_members) < CROSS_SECTIONAL_CONTEXT_MIN_MEMBERS_V2
    ):
        return CrossSectionalContextEvidenceV2(
            **common,
            prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
            status=CrossSectionalContextStatusV2.FEATURE_NOT_READY_MEMBER_COUNT,
            reasons=(
                "ORIGINAL_COUNT_LT_20_OR_POST_EXCLUSION_STRUCTURAL_COUNT_LT_19",
            ),
            m3_ex_target=None,
            shock_scale=None,
            shock_score=None,
            breadth_count=None,
            breadth_denominator=None,
            _factory_token=_FACTORY_TOKEN,
        )

    prior_rows: list[list[Decimal]] = [
        [] for _ in range(FAMILY_C_PRIOR_WINDOW_V2)
    ]
    current_returns: list[Decimal] = []
    closes_by_member: dict[str, list[Decimal]] = {
        member: [] for member in ex_target_members
    }
    for candle in panel.candles:
        member_closes = closes_by_member.get(candle.symbol)
        if member_closes is not None:
            member_closes.append(candle.close)
    try:
        with localcontext(protocol_decimal_context_v2()):
            for member in ex_target_members:
                closes = tuple(closes_by_member[member])
                if len(closes) != FAMILY_C_PANEL_BAR_COUNT_V2:
                    raise CrossSectionalEvidenceContractErrorV2(
                        "canonical panel member slice changed during derivation"
                    )
                prior = tuple(
                    (closes[index] / closes[index - 3]).ln()
                    for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1)
                )
                if len(prior) != FAMILY_C_PRIOR_WINDOW_V2:
                    raise CrossSectionalEvidenceContractErrorV2(
                        "target-excluded prior return window is not exactly 8,640"
                    )
                for index, value in enumerate(prior):
                    prior_rows[index].append(value)
                current_returns.append((closes[-1] / closes[-4]).ln())
            prior_m3 = tuple(_median_decimal(tuple(row)) for row in prior_rows)
            m3_current = _median_decimal(tuple(current_returns))
            shock_scale = _MAD_SCALE * _mad_decimal(prior_m3)
    except DecimalException:
        return CrossSectionalContextEvidenceV2(
            **common,
            prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
            status=CrossSectionalContextStatusV2.DATA_INVALID_ARITHMETIC,
            reasons=("TARGET_EXCLUDED_DECIMAL_ARITHMETIC_INVALID",),
            m3_ex_target=None,
            shock_scale=None,
            shock_score=None,
            breadth_count=None,
            breadth_denominator=None,
            _factory_token=_FACTORY_TOKEN,
        )
    if shock_scale <= 0:
        return CrossSectionalContextEvidenceV2(
            **common,
            prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
            status=CrossSectionalContextStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
            reasons=("TARGET_EXCLUDED_SHOCK_MAD_LE_ZERO",),
            m3_ex_target=None,
            shock_scale=None,
            shock_score=None,
            breadth_count=None,
            breadth_denominator=None,
            _factory_token=_FACTORY_TOKEN,
        )
    with localcontext(protocol_decimal_context_v2()):
        shock_score = abs(m3_current) / shock_scale
    direction = _sign(m3_current)
    breadth_count = sum(
        (direction > 0 and value > 0) or (direction < 0 and value < 0)
        for value in current_returns
    )
    return CrossSectionalContextEvidenceV2(
        **common,
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        status=CrossSectionalContextStatusV2.READY,
        reasons=(
            "TARGET_EXCLUDED_CONTEXT_READY",
            "TARGET_REMOVED_BEFORE_ALL_MARKET_MEDIANS",
        ),
        m3_ex_target=m3_current,
        shock_scale=shock_scale,
        shock_score=shock_score,
        breadth_count=breadth_count,
        breadth_denominator=len(ex_target_members),
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_cross_sectional_context_evidence_v2(
    evidence: CrossSectionalContextEvidenceV2,
) -> bytes:
    if not isinstance(evidence, CrossSectionalContextEvidenceV2):
        raise CrossSectionalEvidenceContractErrorV2(
            "evidence must be CrossSectionalContextEvidenceV2"
        )
    expected = hashlib.sha256(
        _EVIDENCE_DOMAIN
        + canonical_json_line(_evidence_document(evidence, include_evidence_hash=False))
    ).hexdigest()
    if evidence.evidence_sha256 != expected:
        raise CrossSectionalEvidenceContractErrorV2(
            "cross-sectional evidence hash differs from canonical content"
        )
    return canonical_json_line(_evidence_document(evidence, include_evidence_hash=True))


def _common_fields(
    panel: FamilyCCandlePanelV2,
    *,
    target_symbol: str,
    target_present: bool,
    ex_target_members: tuple[str, ...],
    ex_target_member_root_sha256: str,
    ex_target_slice_root_sha256: str,
) -> _CommonFieldsV2:
    ex_target_set = frozenset(ex_target_members)
    ex_target_candles = tuple(
        candle for candle in panel.candles if candle.symbol in ex_target_set
    )
    if not ex_target_candles:
        raise CrossSectionalEvidenceContractErrorV2(
            "target-excluded context requires at least one retained member candle"
        )
    return {
        "target_symbol": target_symbol,
        "target_present": target_present,
        "venue": panel.venue,
        "promoting_plan_sha256": panel.promoting_plan_sha256,
        "source_root_sha256": panel.source_root_sha256,
        "original_universe_root_sha256": panel.universe.universe_root_sha256,
        "original_panel_root_sha256": panel.panel_root_sha256,
        "bar_open_ms": panel.current_bar_open_ms,
        "bar_close_ms": panel.current_bar_close_ms,
        "decision_cutoff_ms": panel.decision_cutoff_ms,
        "latest_source_event_ms": max(
            candle.event_time_ms for candle in ex_target_candles
        ),
        "latest_source_receipt_ms": max(
            candle.receipt_time_ms for candle in ex_target_candles
        ),
        "original_member_count": len(panel.universe.members),
        "ex_target_members": ex_target_members,
        "ex_target_member_root_sha256": ex_target_member_root_sha256,
        "ex_target_slice_root_sha256": ex_target_slice_root_sha256,
    }


def _member_root(
    target_symbol: str,
    target_present: bool,
    ex_target_members: tuple[str, ...],
) -> str:
    return hashlib.sha256(
        _MEMBER_ROOT_DOMAIN
        + canonical_json_line(
            {
                "ex_target_members": list(ex_target_members),
                "schema_version": "r4b_target_excluded_member_set_v2",
                "target_present": target_present,
                "target_symbol": target_symbol,
            }
        )
    ).hexdigest()


def _slice_root(
    panel: FamilyCCandlePanelV2,
    target_symbol: str,
    target_present: bool,
    ex_target_members: tuple[str, ...],
) -> str:
    slices = dict(panel.symbol_slice_sha256s)
    return hashlib.sha256(
        _SLICE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "ex_target_symbol_slices": [
                    {"sha256": slices[member], "symbol": member}
                    for member in ex_target_members
                ],
                "original_universe_root_sha256": (
                    panel.universe.universe_root_sha256
                ),
                "schema_version": "r4b_target_excluded_context_slice_v2",
                "target_present": target_present,
                "target_symbol": target_symbol,
            }
        )
    ).hexdigest()


def _evidence_document(
    value: CrossSectionalContextEvidenceV2,
    *,
    include_evidence_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "breadth_count": value.breadth_count,
        "breadth_denominator": value.breadth_denominator,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "ex_target_member_root_sha256": value.ex_target_member_root_sha256,
        "ex_target_members": list(value.ex_target_members),
        "ex_target_slice_root_sha256": value.ex_target_slice_root_sha256,
        "latest_source_event_ms": value.latest_source_event_ms,
        "latest_source_receipt_ms": value.latest_source_receipt_ms,
        "m3_ex_target": (
            None if value.m3_ex_target is None else str(value.m3_ex_target)
        ),
        "original_member_count": value.original_member_count,
        "original_panel_root_sha256": value.original_panel_root_sha256,
        "original_universe_root_sha256": value.original_universe_root_sha256,
        "prior_observation_count": value.prior_observation_count,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "reasons": list(value.reasons),
        "rule_version": value.rule_version,
        "schema_version": "r4b_target_excluded_context_evidence_v2",
        "shock_scale": None if value.shock_scale is None else str(value.shock_scale),
        "shock_score": None if value.shock_score is None else str(value.shock_score),
        "source_root_sha256": value.source_root_sha256,
        "status": value.status.value,
        "target_present": value.target_present,
        "target_symbol": value.target_symbol,
        "venue": value.venue.value,
    }
    if include_evidence_hash:
        document["evidence_sha256"] = value.evidence_sha256
    return document


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise CrossSectionalEvidenceContractErrorV2(
            "target-excluded median requires at least one value"
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


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise CrossSectionalEvidenceContractErrorV2(
            "target_symbol must be a normalized USDT symbol"
        )


def _validate_member_set(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values:
        raise CrossSectionalEvidenceContractErrorV2(
            "ex_target_members must be a non-empty immutable tuple"
        )
    if values != tuple(sorted(values, key=lambda item: item.encode("utf-8"))):
        raise CrossSectionalEvidenceContractErrorV2(
            "ex_target_members must use canonical UTF-8 order"
        )
    if len(values) != len(set(values)):
        raise CrossSectionalEvidenceContractErrorV2(
            "ex_target_members cannot contain duplicates"
        )
    for value in values:
        _validate_symbol(value)


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise CrossSectionalEvidenceContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    if any(
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        for value in values
    ):
        raise CrossSectionalEvidenceContractErrorV2(
            "reason must be a bounded normalized identity"
        )


def _validate_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CrossSectionalEvidenceContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _validate_nonnegative_int(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise CrossSectionalEvidenceContractErrorV2(
            f"{name} must be a nonnegative integer"
        )


def _is_finite_decimal(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0
